# Openable Navbar Tabs Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Split Vireo's navbar into always-shown "linger pages" and openable/closeable "tabs" (settings, workspace, lightroom, shortcuts, keywords, duplicates, logs), with per-workspace persisted state.

**Architecture:** Server-rendered navbar reads a per-workspace `open_tabs` JSON column from SQLite. Each openable page route auto-adds its nav-id to `open_tabs` so direct URL visits / keyboard shortcuts "just work". Two API endpoints (open/close) are idempotent and validate against a canonical `OPENABLE_NAV_IDS` set. A Tools dropdown in the navbar acts as a toggle (checkmark = open).

**Tech Stack:** Python 3, Flask, Jinja2, SQLite, vanilla JS. TDD with pytest. See `docs/plans/2026-04-25-openable-tabs-design.md` for full design rationale.

**Key constraint:** The navbar already has a `nav_order` per-workspace personalization (drag-to-reorder via `config_overrides.nav_order`, set via `PUT /api/workspaces/active/nav-order`). After this change, `nav_order` only governs the linger-page section. Openable tabs are ordered by `open_tabs`. Dragging a tab does nothing (we leave the existing `draggable=true` on tabs but don't write tab order to `nav_order`).

---

## Phase 1: Database layer

### Task 1: Add `open_tabs` column to `workspaces` table with backfill

**Files:**
- Modify: `vireo/db.py:193-200` (workspaces CREATE TABLE)
- Modify: `vireo/db.py` somewhere after the `_create_tables` schema block (~line 463) — add an idempotent ALTER for existing DBs
- Test: `tests/test_workspaces.py` (append new tests at end)

**Step 1: Write the failing test**

Append to `tests/test_workspaces.py`:

```python
def test_workspaces_has_open_tabs_column(db):
    cols = [r[1] for r in db.conn.execute("PRAGMA table_info(workspaces)").fetchall()]
    assert "open_tabs" in cols


def test_existing_workspaces_get_default_open_tabs_on_migration(tmp_path):
    """A pre-existing workspaces table without open_tabs should be backfilled."""
    import sqlite3, json as _json
    db_path = tmp_path / "legacy.db"
    # Hand-craft a legacy DB without the open_tabs column
    conn = sqlite3.connect(str(db_path))
    conn.execute(
        "CREATE TABLE workspaces (id INTEGER PRIMARY KEY, name TEXT NOT NULL UNIQUE, "
        "config_overrides TEXT, ui_state TEXT, created_at TEXT, last_opened_at TEXT)"
    )
    conn.execute("INSERT INTO workspaces (name) VALUES ('Legacy')")
    conn.commit()
    conn.close()

    # Open via Database — migration should run
    from db import Database
    d = Database(str(db_path))

    cols = [r[1] for r in d.conn.execute("PRAGMA table_info(workspaces)").fetchall()]
    assert "open_tabs" in cols

    # Existing rows should be backfilled with the defaults
    row = d.conn.execute(
        "SELECT open_tabs FROM workspaces WHERE name = 'Legacy'"
    ).fetchone()
    assert row[0] is not None
    assert _json.loads(row[0]) == ["settings", "workspace", "lightroom"]
```

**Step 2: Run tests to verify they fail**

```
python -m pytest tests/test_workspaces.py::test_workspaces_has_open_tabs_column tests/test_workspaces.py::test_existing_workspaces_get_default_open_tabs_on_migration -v
```
Expected: both FAIL ("no such column: open_tabs" or similar).

**Step 3: Add the column to the schema**

Edit `vireo/db.py` workspaces block (lines 193–200):

```python
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

That handles fresh DBs. For pre-existing DBs, add a migration block after the existing embedding migration (~line 462, before `self.conn.commit()` on line 463). Use the same try/except-on-SELECT pattern already in the file:

```python
        # Migration: add open_tabs column to existing workspaces tables, with defaults
        try:
            self.conn.execute("SELECT open_tabs FROM workspaces LIMIT 0")
        except sqlite3.OperationalError:
            self.conn.execute("ALTER TABLE workspaces ADD COLUMN open_tabs TEXT")
            self.conn.execute(
                "UPDATE workspaces SET open_tabs = ? WHERE open_tabs IS NULL",
                (json.dumps(["settings", "workspace", "lightroom"]),),
            )
        self.conn.commit()
```

**Step 4: Run tests to verify they pass**

```
python -m pytest tests/test_workspaces.py::test_workspaces_has_open_tabs_column tests/test_workspaces.py::test_existing_workspaces_get_default_open_tabs_on_migration -v
```
Expected: both PASS.

Also run the existing workspace tests to confirm no regression:
```
python -m pytest tests/test_workspaces.py -v
```
Expected: all PASS.

**Step 5: Commit**

```bash
git add vireo/db.py tests/test_workspaces.py
git commit -m "db: add open_tabs column to workspaces with default backfill"
```

---

### Task 2: Default `open_tabs` for newly created workspaces

**Files:**
- Modify: `vireo/db.py:537-553` (`create_workspace`)
- Test: `tests/test_workspaces.py`

**Step 1: Write the failing test**

Append to `tests/test_workspaces.py`:

```python
def test_new_workspace_gets_default_open_tabs(db):
    import json as _json
    ws_id = db.create_workspace("Fresh")
    row = db.conn.execute(
        "SELECT open_tabs FROM workspaces WHERE id = ?", (ws_id,)
    ).fetchone()
    assert row["open_tabs"] is not None
    assert _json.loads(row["open_tabs"]) == ["settings", "workspace", "lightroom"]
```

**Step 2: Run test to verify it fails**

```
python -m pytest tests/test_workspaces.py::test_new_workspace_gets_default_open_tabs -v
```
Expected: FAIL — `row["open_tabs"]` is None for newly created workspaces.

**Step 3: Update `create_workspace`**

Edit `vireo/db.py:537-553`. Change the INSERT to include `open_tabs`:

```python
    DEFAULT_OPEN_TABS = ["settings", "workspace", "lightroom"]

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
        self.conn.commit()
        workspace_id = cur.lastrowid
        self._new_images_cache.invalidate_workspaces(self._db_path, [workspace_id])
        return workspace_id
```

(Define `DEFAULT_OPEN_TABS` as a class attribute on `Database`. Place it just inside `class Database:` near the existing `_UNSET` sentinel use.)

**Step 4: Run test to verify it passes**

```
python -m pytest tests/test_workspaces.py::test_new_workspace_gets_default_open_tabs -v
```
Expected: PASS.

**Step 5: Commit**

```bash
git add vireo/db.py tests/test_workspaces.py
git commit -m "db: seed new workspaces with default open_tabs"
```

---

### Task 3: `get_open_tabs()` method

**Files:**
- Modify: `vireo/db.py` — add new method near `get_workspace_active_labels` (around line 615)
- Test: `tests/test_workspaces.py`

**Step 1: Write the failing test**

```python
def test_get_open_tabs_returns_default_for_new_workspace(db):
    ws_id = db.create_workspace("WS")
    db.set_active_workspace(ws_id)
    assert db.get_open_tabs() == ["settings", "workspace", "lightroom"]


def test_get_open_tabs_returns_empty_list_when_null(db):
    ws_id = db.create_workspace("WS2")
    db.conn.execute("UPDATE workspaces SET open_tabs = NULL WHERE id = ?", (ws_id,))
    db.conn.commit()
    db.set_active_workspace(ws_id)
    assert db.get_open_tabs() == []
```

**Step 2: Run tests to verify they fail**

```
python -m pytest tests/test_workspaces.py::test_get_open_tabs_returns_default_for_new_workspace tests/test_workspaces.py::test_get_open_tabs_returns_empty_list_when_null -v
```
Expected: both FAIL — `AttributeError: 'Database' object has no attribute 'get_open_tabs'`.

**Step 3: Implement `get_open_tabs`**

Add to `vireo/db.py` near other workspace methods:

```python
    def get_open_tabs(self):
        """Return the active workspace's list of open tab nav-ids in display order."""
        ws = self.get_workspace(self._ws_id())
        if not ws or not ws["open_tabs"]:
            return []
        try:
            value = json.loads(ws["open_tabs"]) if isinstance(ws["open_tabs"], str) else ws["open_tabs"]
            return value if isinstance(value, list) else []
        except (json.JSONDecodeError, TypeError):
            return []
```

**Step 4: Run tests to verify they pass**

```
python -m pytest tests/test_workspaces.py::test_get_open_tabs_returns_default_for_new_workspace tests/test_workspaces.py::test_get_open_tabs_returns_empty_list_when_null -v
```
Expected: both PASS.

**Step 5: Commit**

```bash
git add vireo/db.py tests/test_workspaces.py
git commit -m "db: add get_open_tabs() reader for active workspace"
```

---

### Task 4: `OPENABLE_NAV_IDS` constant + `open_tab()` method

**Files:**
- Modify: `vireo/db.py` — add the constant + method
- Test: `tests/test_workspaces.py`

**Step 1: Write the failing tests**

```python
def test_openable_nav_ids_constant():
    from db import OPENABLE_NAV_IDS
    assert OPENABLE_NAV_IDS == frozenset({
        "settings", "workspace", "lightroom",
        "shortcuts", "keywords", "duplicates", "logs",
    })


def test_open_tab_appends_to_end(db):
    ws_id = db.create_workspace("WS")
    db.set_active_workspace(ws_id)
    # Start from defaults: ["settings", "workspace", "lightroom"]
    db.open_tab("keywords")
    assert db.get_open_tabs() == ["settings", "workspace", "lightroom", "keywords"]


def test_open_tab_is_idempotent(db):
    ws_id = db.create_workspace("WS")
    db.set_active_workspace(ws_id)
    db.open_tab("keywords")
    db.open_tab("keywords")
    assert db.get_open_tabs().count("keywords") == 1


def test_open_tab_rejects_non_openable_navid(db):
    ws_id = db.create_workspace("WS")
    db.set_active_workspace(ws_id)
    with pytest.raises(ValueError):
        db.open_tab("browse")  # browse is a linger page, not openable
```

**Step 2: Run tests to verify they fail**

```
python -m pytest tests/test_workspaces.py::test_openable_nav_ids_constant tests/test_workspaces.py::test_open_tab_appends_to_end tests/test_workspaces.py::test_open_tab_is_idempotent tests/test_workspaces.py::test_open_tab_rejects_non_openable_navid -v
```
Expected: all FAIL.

**Step 3: Add the constant and method**

At module top of `vireo/db.py` (near other module-level definitions):

```python
OPENABLE_NAV_IDS = frozenset({
    "settings", "workspace", "lightroom",
    "shortcuts", "keywords", "duplicates", "logs",
})
```

Add method to `Database`:

```python
    def open_tab(self, nav_id):
        """Append nav_id to the active workspace's open_tabs if not present.

        Raises ValueError if nav_id is not in OPENABLE_NAV_IDS.
        Returns the new list.
        """
        if nav_id not in OPENABLE_NAV_IDS:
            raise ValueError(f"{nav_id!r} is not an openable nav id")
        tabs = self.get_open_tabs()
        if nav_id not in tabs:
            tabs.append(nav_id)
            self.conn.execute(
                "UPDATE workspaces SET open_tabs = ? WHERE id = ?",
                (json.dumps(tabs), self._ws_id()),
            )
            self.conn.commit()
        return tabs
```

**Step 4: Run tests to verify they pass**

```
python -m pytest tests/test_workspaces.py -v -k "openable or open_tab"
```
Expected: PASS.

**Step 5: Commit**

```bash
git add vireo/db.py tests/test_workspaces.py
git commit -m "db: add OPENABLE_NAV_IDS constant and open_tab() method"
```

---

### Task 5: `close_tab()` method

**Files:**
- Modify: `vireo/db.py`
- Test: `tests/test_workspaces.py`

**Step 1: Write the failing tests**

```python
def test_close_tab_removes_from_list(db):
    ws_id = db.create_workspace("WS")
    db.set_active_workspace(ws_id)
    db.close_tab("settings")
    assert db.get_open_tabs() == ["workspace", "lightroom"]


def test_close_tab_idempotent_when_not_open(db):
    ws_id = db.create_workspace("WS")
    db.set_active_workspace(ws_id)
    db.close_tab("keywords")  # not open — should be no-op
    assert db.get_open_tabs() == ["settings", "workspace", "lightroom"]


def test_close_tab_rejects_non_openable_navid(db):
    ws_id = db.create_workspace("WS")
    db.set_active_workspace(ws_id)
    with pytest.raises(ValueError):
        db.close_tab("browse")
```

**Step 2: Run tests to verify they fail**

```
python -m pytest tests/test_workspaces.py -v -k "close_tab"
```
Expected: all FAIL — `close_tab` doesn't exist.

**Step 3: Implement `close_tab`**

```python
    def close_tab(self, nav_id):
        """Remove nav_id from the active workspace's open_tabs if present.

        Raises ValueError if nav_id is not in OPENABLE_NAV_IDS.
        Returns the new list.
        """
        if nav_id not in OPENABLE_NAV_IDS:
            raise ValueError(f"{nav_id!r} is not an openable nav id")
        tabs = self.get_open_tabs()
        if nav_id in tabs:
            tabs = [t for t in tabs if t != nav_id]
            self.conn.execute(
                "UPDATE workspaces SET open_tabs = ? WHERE id = ?",
                (json.dumps(tabs), self._ws_id()),
            )
            self.conn.commit()
        return tabs
```

**Step 4: Run tests to verify they pass**

```
python -m pytest tests/test_workspaces.py -v -k "close_tab"
```
Expected: PASS.

**Step 5: Commit**

```bash
git add vireo/db.py tests/test_workspaces.py
git commit -m "db: add close_tab() method"
```

---

### Task 6: Per-workspace isolation of open_tabs

**Files:**
- Test: `tests/test_workspaces.py`

This is a behavior test, not a code change — verifies that two workspaces have independent `open_tabs`.

**Step 1: Write the test**

```python
def test_open_tabs_are_per_workspace(db):
    ws_a = db.create_workspace("A")
    ws_b = db.create_workspace("B")

    db.set_active_workspace(ws_a)
    db.open_tab("keywords")

    db.set_active_workspace(ws_b)
    assert "keywords" not in db.get_open_tabs()
    db.open_tab("logs")

    db.set_active_workspace(ws_a)
    tabs = db.get_open_tabs()
    assert "keywords" in tabs
    assert "logs" not in tabs
```

**Step 2: Run test to verify it passes** (should pass on first run since the methods already use `_ws_id()`)

```
python -m pytest tests/test_workspaces.py::test_open_tabs_are_per_workspace -v
```
Expected: PASS. If it fails, investigate.

**Step 3: Commit**

```bash
git add tests/test_workspaces.py
git commit -m "test: verify open_tabs are isolated per workspace"
```

---

## Phase 2: API layer

### Task 7: `POST /api/workspace/tabs/open` endpoint

**Files:**
- Modify: `vireo/app.py` — add the route near other `/api/workspaces/active/*` routes (~line 2250)
- Test: `vireo/tests/test_tabs_api.py` (new file)

**Step 1: Write the failing tests**

Create `vireo/tests/test_tabs_api.py`:

```python
def test_open_tab_endpoint_appends(app_and_db):
    app, db = app_and_db
    client = app.test_client()
    r = client.post("/api/workspace/tabs/open", json={"nav_id": "keywords"})
    assert r.status_code == 200
    body = r.get_json()
    assert "keywords" in body["open_tabs"]


def test_open_tab_endpoint_rejects_unknown_navid(app_and_db):
    app, db = app_and_db
    client = app.test_client()
    r = client.post("/api/workspace/tabs/open", json={"nav_id": "browse"})
    assert r.status_code == 400


def test_open_tab_endpoint_idempotent(app_and_db):
    app, db = app_and_db
    client = app.test_client()
    client.post("/api/workspace/tabs/open", json={"nav_id": "logs"})
    r = client.post("/api/workspace/tabs/open", json={"nav_id": "logs"})
    assert r.status_code == 200
    body = r.get_json()
    assert body["open_tabs"].count("logs") == 1
```

**Step 2: Run tests to verify they fail**

```
python -m pytest vireo/tests/test_tabs_api.py -v
```
Expected: all FAIL — endpoint doesn't exist (404).

**Step 3: Add the endpoint**

In `vireo/app.py`, near `/api/workspaces/active/nav-order` (~line 2250). Look for `from db import OPENABLE_NAV_IDS` (add it near the top imports if not already imported) and add:

```python
    @app.route("/api/workspace/tabs/open", methods=["POST"])
    def api_open_tab():
        from db import OPENABLE_NAV_IDS
        db = _get_db()
        body = request.get_json(silent=True) or {}
        nav_id = body.get("nav_id")
        if nav_id not in OPENABLE_NAV_IDS:
            return json_error("nav_id is not openable", 400)
        tabs = db.open_tab(nav_id)
        return jsonify({"ok": True, "open_tabs": tabs})
```

**Step 4: Run tests to verify they pass**

```
python -m pytest vireo/tests/test_tabs_api.py -v
```
Expected: PASS.

**Step 5: Commit**

```bash
git add vireo/app.py vireo/tests/test_tabs_api.py
git commit -m "api: add POST /api/workspace/tabs/open"
```

---

### Task 8: `POST /api/workspace/tabs/close` endpoint

**Files:**
- Modify: `vireo/app.py`
- Test: `vireo/tests/test_tabs_api.py`

**Step 1: Write the failing tests**

Append to `vireo/tests/test_tabs_api.py`:

```python
def test_close_tab_endpoint_removes(app_and_db):
    app, db = app_and_db
    client = app.test_client()
    r = client.post("/api/workspace/tabs/close", json={"nav_id": "settings"})
    assert r.status_code == 200
    assert "settings" not in r.get_json()["open_tabs"]


def test_close_tab_endpoint_idempotent_when_not_open(app_and_db):
    app, db = app_and_db
    client = app.test_client()
    client.post("/api/workspace/tabs/close", json={"nav_id": "settings"})
    r = client.post("/api/workspace/tabs/close", json={"nav_id": "settings"})
    assert r.status_code == 200


def test_close_tab_endpoint_rejects_unknown_navid(app_and_db):
    app, db = app_and_db
    client = app.test_client()
    r = client.post("/api/workspace/tabs/close", json={"nav_id": "browse"})
    assert r.status_code == 400
```

**Step 2: Run tests to verify they fail**

```
python -m pytest vireo/tests/test_tabs_api.py -v -k "close"
```
Expected: FAIL.

**Step 3: Add the endpoint**

```python
    @app.route("/api/workspace/tabs/close", methods=["POST"])
    def api_close_tab():
        from db import OPENABLE_NAV_IDS
        db = _get_db()
        body = request.get_json(silent=True) or {}
        nav_id = body.get("nav_id")
        if nav_id not in OPENABLE_NAV_IDS:
            return json_error("nav_id is not openable", 400)
        tabs = db.close_tab(nav_id)
        return jsonify({"ok": True, "open_tabs": tabs})
```

**Step 4: Run tests to verify they pass**

```
python -m pytest vireo/tests/test_tabs_api.py -v
```
Expected: PASS.

**Step 5: Commit**

```bash
git add vireo/app.py vireo/tests/test_tabs_api.py
git commit -m "api: add POST /api/workspace/tabs/close"
```

---

### Task 9: Auto-open on direct URL visit

**Files:**
- Modify: `vireo/app.py:719-770` (lightroom_page, settings, workspace_page, shortcuts_page, keywords_page, duplicates_page) and `:8876-8878` (logs_page)
- Test: `vireo/tests/test_tabs_api.py`

**Step 1: Write the failing test**

```python
def test_visiting_lightroom_url_auto_opens_tab(app_and_db):
    app, db = app_and_db
    client = app.test_client()
    # Close lightroom first
    client.post("/api/workspace/tabs/close", json={"nav_id": "lightroom"})
    assert "lightroom" not in db.get_open_tabs()
    # Visit the page
    r = client.get("/lightroom")
    assert r.status_code == 200
    assert "lightroom" in db.get_open_tabs()


def test_visiting_logs_url_auto_opens_tab(app_and_db):
    app, db = app_and_db
    client = app.test_client()
    client.post("/api/workspace/tabs/close", json={"nav_id": "logs"})
    r = client.get("/logs")
    assert r.status_code == 200
    assert "logs" in db.get_open_tabs()
```

**Step 2: Run tests to verify they fail**

```
python -m pytest vireo/tests/test_tabs_api.py -v -k "auto_opens"
```
Expected: FAIL — pages render but don't update `open_tabs`.

**Step 3: Add a small helper and call it from each openable page route**

In `vireo/app.py`, near the top of `create_app` (or near the page routes), define:

```python
    def _auto_open_tab(nav_id):
        """Best-effort: append nav_id to the active workspace's open_tabs.

        Called from openable page routes so direct URL visits / shortcuts
        keep the navbar consistent. Errors are swallowed (the page still renders).
        """
        try:
            _get_db().open_tab(nav_id)
        except Exception:
            log.exception("Failed to auto-open tab %r", nav_id)
```

Then update each openable page route. For lightroom (line 719):

```python
    @app.route("/lightroom")
    def lightroom_page():
        _auto_open_tab("lightroom")
        return render_template("lightroom.html")
```

Repeat the `_auto_open_tab(...)` call for: `workspace_page` ("workspace"), `settings` ("settings"), `shortcuts_page` ("shortcuts"), `keywords_page` ("keywords"), `duplicates_page` ("duplicates"), `logs_page` ("logs").

**Step 4: Run tests to verify they pass**

```
python -m pytest vireo/tests/test_tabs_api.py -v
```
Expected: PASS.

**Step 5: Commit**

```bash
git add vireo/app.py vireo/tests/test_tabs_api.py
git commit -m "api: auto-open tab when openable page is visited directly"
```

---

## Phase 3: Template / UI layer

### Task 10: Inject `open_tabs` and constants into all templates via context_processor

**Files:**
- Modify: `vireo/app.py` — add a `@app.context_processor` near the top of `create_app` body
- Test: manual / smoke; tested implicitly by Task 11

**Step 1: Add the context processor**

In `vireo/app.py`, near other `@app.before_request` blocks (~line 540):

```python
    @app.context_processor
    def _inject_navbar_state():
        """Make open-tabs state available to every rendered template."""
        from db import OPENABLE_NAV_IDS
        try:
            tabs = _get_db().get_open_tabs()
        except Exception:
            tabs = []
        # Canonical display order for the Tools dropdown
        TOOLS_ORDER = ["settings", "workspace", "lightroom",
                       "shortcuts", "keywords", "duplicates", "logs"]
        TAB_LABELS = {
            "settings": "Settings",
            "workspace": "Workspace",
            "lightroom": "Lightroom",
            "shortcuts": "Shortcuts",
            "keywords": "Keywords",
            "duplicates": "Duplicates",
            "logs": "Logs",
        }
        TAB_HREFS = {
            "settings": "/settings",
            "workspace": "/workspace",
            "lightroom": "/lightroom",
            "shortcuts": "/shortcuts",
            "keywords": "/keywords",
            "duplicates": "/duplicates",
            "logs": "/logs",
        }
        return {
            "open_tabs": tabs,
            "openable_nav_ids": list(OPENABLE_NAV_IDS),
            "tools_order": TOOLS_ORDER,
            "tab_labels": TAB_LABELS,
            "tab_hrefs": TAB_HREFS,
        }
```

**Step 2: Smoke test by booting the app**

```
python vireo/app.py --db /tmp/vireo-tabs-test.db --port 8765 &
sleep 2
curl -s http://localhost:8765/browse -o /dev/null -w '%{http_code}\n'
kill %1
```
Expected: `200`. (No template change yet — just verifying context_processor doesn't break rendering.)

**Step 3: Commit**

```bash
git add vireo/app.py
git commit -m "app: inject open_tabs and tab metadata into template context"
```

---

### Task 11: Render linger pages and tabs separately in the navbar

**Files:**
- Modify: `vireo/templates/_navbar.html:1087-1105` (replace the hard-coded openable links with a Jinja loop driven by `open_tabs`)
- Modify: `vireo/templates/_navbar.html` CSS block — add styles for `.nav-tab-close` and `.nav-divider`

**Step 1: Replace the hard-coded openable links**

The current navbar (`_navbar.html:1087-1105`) lists all 19 nav links. Keep the linger pages exactly as they are. Remove the links for the 7 openable nav-ids (`lightroom`, `workspace`, `keywords`, `shortcuts`, `duplicates`, `settings`) from their current positions. The `logs` icon link at line 1115 stays where it is (it's the icon-style entry, not the openable tab — but we'll handle it in Task 13 with the Tools dropdown logic).

After the linger-page links and *before* the `<span class="nav-spacer">` at line 1106, insert:

```jinja
  <span class="nav-tab-divider" aria-hidden="true"></span>
  {% for tab_id in open_tabs %}
    {% if tab_id in tab_labels %}
    <a href="{{ tab_hrefs[tab_id] }}"
       data-nav-id="{{ tab_id }}"
       data-tab="1"
       class="nav-tab">
      {{ tab_labels[tab_id] }}
      <span class="nav-tab-close"
            title="Close tab"
            onclick="event.preventDefault(); event.stopPropagation(); closeTab('{{ tab_id }}'); return false;">×</span>
    </a>
    {% endif %}
  {% endfor %}
  <button type="button"
          class="nav-tools-btn"
          data-testid="nav-tools-btn"
          onclick="toggleToolsMenu(event)">+ Tools ▾</button>
  <div class="nav-tools-menu" id="navToolsMenu" hidden>
    {% for tab_id in tools_order %}
      <button type="button"
              class="nav-tools-item"
              data-nav-id="{{ tab_id }}"
              data-open="{{ '1' if tab_id in open_tabs else '0' }}"
              onclick="toggleTabFromMenu('{{ tab_id }}')">
        <span class="nav-tools-check">{% if tab_id in open_tabs %}✓{% else %}&nbsp;{% endif %}</span>
        {{ tab_labels[tab_id] }}
      </button>
    {% endfor %}
  </div>
```

**Step 2: Add the CSS**

In the `<style>` block at the top of `_navbar.html` (find an appropriate spot near other `.navbar` rules, e.g. before line 1071):

```css
.navbar .nav-tab { position: relative; }
.navbar .nav-tab-close {
  display: none;
  margin-left: 6px;
  padding: 0 4px;
  font-size: 14px;
  line-height: 1;
  color: var(--muted, #999);
  border-radius: 3px;
  cursor: pointer;
}
.navbar .nav-tab:hover .nav-tab-close,
.navbar .nav-tab.active .nav-tab-close { display: inline-block; }
.navbar .nav-tab-close:hover { background: var(--hover, rgba(255,255,255,0.1)); color: var(--text); }
.navbar .nav-tab-divider {
  display: inline-block;
  width: 1px;
  height: 18px;
  margin: 0 8px;
  background: var(--border, rgba(255,255,255,0.15));
  vertical-align: middle;
}
.navbar .nav-tools-btn {
  background: none;
  border: 1px dashed var(--border, rgba(255,255,255,0.2));
  color: var(--muted, #999);
  padding: 2px 8px;
  border-radius: 4px;
  cursor: pointer;
  font: inherit;
  margin-left: 4px;
}
.navbar .nav-tools-btn:hover { color: var(--text); border-color: var(--text); }
.nav-tools-menu {
  position: absolute;
  top: 36px;
  background: var(--bg-elev, #2a2a2a);
  border: 1px solid var(--border, rgba(255,255,255,0.15));
  border-radius: 6px;
  padding: 4px;
  z-index: 1000;
  min-width: 160px;
  box-shadow: 0 4px 12px rgba(0,0,0,0.25);
}
.nav-tools-menu[hidden] { display: none; }
.nav-tools-item {
  display: flex;
  align-items: center;
  width: 100%;
  background: none;
  border: 0;
  color: var(--text);
  text-align: left;
  padding: 6px 8px;
  font: inherit;
  cursor: pointer;
  border-radius: 4px;
}
.nav-tools-item:hover { background: var(--hover, rgba(255,255,255,0.08)); }
.nav-tools-check { display: inline-block; width: 16px; color: var(--accent); }
```

**Step 3: Boot the app and verify visually**

```
python vireo/app.py --db /tmp/vireo-tabs-test.db --port 8765 &
sleep 2
# Open http://localhost:8765/browse in a browser
# Expected: linger nav links + a divider + Settings/Workspace/Lightroom tabs (each with × on hover) + a "+ Tools ▾" button
# Hover over a tab: the × should appear
# Click on "Settings" tab: navigates to /settings, the settings tab is highlighted active and shows the × always
kill %1
```

**Step 4: Commit**

```bash
git add vireo/templates/_navbar.html
git commit -m "navbar: render openable tabs separately with × close button"
```

---

### Task 12: `closeTab`, `openTab`, `toggleToolsMenu`, `toggleTabFromMenu` JS

**Files:**
- Modify: `vireo/templates/_navbar.html` — add a `<script>` block with these handlers (near the existing nav-reorder JS, ~line 1399)

**Step 1: Add the handlers**

Insert near the existing nav reorder script:

```javascript
<script>
/* ---------- Openable tabs: open/close + Tools dropdown ---------- */
(function() {
  // Treat these as "linger" pages; everything else is an openable tab
  var LINGER_PAGES = ["pipeline","jobs","pipeline-review","review","cull","misses",
                      "highlights","browse","map","variants","dashboard","audit","compare"];

  function postJSON(url, body) {
    return fetch(url, {
      method: 'POST',
      headers: {'Content-Type': 'application/json'},
      body: JSON.stringify(body || {}),
    }).then(function(r) { return r.json(); });
  }

  // Compute current page nav-id from the URL path
  function currentNavId() {
    var p = window.location.pathname;
    if (p.startsWith('/pipeline/review')) return 'pipeline-review';
    if (p === '/' || p.startsWith('/browse')) return 'browse';
    var seg = p.split('/')[1] || '';
    return seg;
  }

  window.closeTab = function(navId) {
    postJSON('/api/workspace/tabs/close', {nav_id: navId}).then(function(res) {
      if (currentNavId() === navId) {
        // Navigate to the next remaining tab, or fall back to /browse
        var next = (res.open_tabs || [])[0];
        window.location.href = next ? ('/' + next) : '/browse';
      } else {
        window.location.reload();
      }
    });
  };

  window.openTab = function(navId) {
    postJSON('/api/workspace/tabs/open', {nav_id: navId}).then(function() {
      window.location.href = '/' + navId;
    });
  };

  window.toggleTabFromMenu = function(navId) {
    var item = document.querySelector('.nav-tools-item[data-nav-id="' + navId + '"]');
    var isOpen = item && item.dataset.open === '1';
    if (isOpen) {
      // Toggle off — remove tab. If user is currently on it, navigate away first.
      window.closeTab(navId);
    } else {
      window.openTab(navId);
    }
  };

  window.toggleToolsMenu = function(ev) {
    if (ev) ev.stopPropagation();
    var menu = document.getElementById('navToolsMenu');
    if (!menu) return;
    if (menu.hasAttribute('hidden')) {
      menu.removeAttribute('hidden');
      // Position it under the button
      var btn = document.querySelector('.nav-tools-btn');
      var rect = btn.getBoundingClientRect();
      menu.style.left = rect.left + 'px';
      // Close on outside click
      setTimeout(function() {
        document.addEventListener('click', closeMenuOnOutside, {once: true});
      }, 0);
    } else {
      menu.setAttribute('hidden', '');
    }
  };

  function closeMenuOnOutside(e) {
    var menu = document.getElementById('navToolsMenu');
    if (!menu) return;
    if (e.target.closest('.nav-tools-menu, .nav-tools-btn')) {
      // re-arm the listener
      document.addEventListener('click', closeMenuOnOutside, {once: true});
      return;
    }
    menu.setAttribute('hidden', '');
  }
})();
</script>
```

**Step 2: Smoke test in the browser**

```
python vireo/app.py --db /tmp/vireo-tabs-test.db --port 8765 &
sleep 2
# Open http://localhost:8765/browse and verify:
# 1. Click "+ Tools ▾" → dropdown appears with checkmarks next to settings/workspace/lightroom
# 2. Click "Keywords" in dropdown → navigates to /keywords; new "Keywords" tab in navbar
# 3. Hover over the Keywords tab and click × → returns to /browse (since you were on it)
# 4. Direct URL: visit /shortcuts → "Shortcuts" tab now appears
# 5. Click × on "Settings" tab while on /browse → tab disappears, you stay on /browse
kill %1
```

**Step 3: Commit**

```bash
git add vireo/templates/_navbar.html
git commit -m "navbar: client-side open/close/toggle handlers and Tools menu"
```

---

### Task 13: Exclude openable tabs from `nav_order` drag-reorder

**Files:**
- Modify: `vireo/templates/_navbar.html:1432-1508` (the `initNavDragDrop` IIFE)

The existing drag-and-drop reorders ALL nav links and saves the order to `nav_order`. After our change, openable tabs are governed by `open_tabs`, not `nav_order`. The simplest fix: exclude tabs (`a[data-tab="1"]`) from drag-handler attachment, and drop them from the `saveOrder` payload.

**Step 1: Modify `getNavLinks` and `saveOrder`**

```javascript
    function getNavLinks() {
      // Linger pages only — exclude openable tabs (which are governed by open_tabs).
      return Array.from(navbar.querySelectorAll('a[data-nav-id]:not([data-tab])'));
    }

    function saveOrder() {
      var order = getNavLinks().map(function(a) { return a.dataset.navId; });
      fetch('/api/workspaces/active/nav-order', {
        method: 'PUT',
        headers: {'Content-Type': 'application/json'},
        body: JSON.stringify({nav_order: order})
      });
    }
```

Also update `reorderNavLinks` (function above) so it only operates on linger links (otherwise legacy `nav_order` entries containing openable nav-ids could shuffle tabs unpredictably):

```javascript
  function reorderNavLinks(order) {
    var navbar = document.querySelector('.navbar');
    var divider = navbar.querySelector('.nav-tab-divider') || navbar.querySelector('.nav-spacer');
    var links = {};
    navbar.querySelectorAll('a[data-nav-id]:not([data-tab])').forEach(function(a) {
      links[a.dataset.navId] = a;
    });
    order.forEach(function(id) {
      if (links[id]) {
        navbar.insertBefore(links[id], divider);
        delete links[id];
      }
    });
    Object.keys(links).forEach(function(id) {
      navbar.insertBefore(links[id], divider);
    });
  }
```

**Step 2: Smoke test**

```
python vireo/app.py --db /tmp/vireo-tabs-test.db --port 8765 &
sleep 2
# In a browser:
# 1. Drag "Cull" before "Browse" → linger order changes, tabs untouched
# 2. Reload → linger order persists, tabs still in open_tabs order
# 3. Try to drag a tab — nothing should happen (the handler isn't attached)
kill %1
```

**Step 3: Commit**

```bash
git add vireo/templates/_navbar.html
git commit -m "navbar: exclude openable tabs from drag-reorder/nav_order"
```

---

## Phase 4: Verification

### Task 14: Run the full project test suite

**Step 1: Run tests**

From the worktree root:

```
python -m pytest tests/test_workspaces.py vireo/tests/test_db.py vireo/tests/test_app.py vireo/tests/test_photos_api.py vireo/tests/test_edits_api.py vireo/tests/test_jobs_api.py vireo/tests/test_darktable_api.py vireo/tests/test_config.py vireo/tests/test_tabs_api.py -v
```
Expected: all PASS (modulo the [Pre-existing test failures](memory) — confirm any failures match the known list before treating them as regressions).

**Step 2: Boot the app and walk through the user flow**

```
python vireo/app.py --db /tmp/vireo-tabs-final.db --port 8765 &
sleep 2
```

In a browser:
1. **Default state**: navbar shows linger pages + divider + Settings/Workspace/Lightroom tabs + `+ Tools ▾`.
2. **Open**: click `+ Tools ▾` → click "Keywords" → URL is `/keywords`, new tab visible.
3. **Close while on the tab**: hover over Keywords tab → click × → redirects to next remaining tab (or `/browse`).
4. **Close while NOT on the tab**: navigate to `/browse`, hover Settings tab → click × → tab disappears, you're still on `/browse`.
5. **Re-open from menu**: `+ Tools ▾` → click "Settings" → navigates to `/settings`, tab reappears at the end of the tab list.
6. **Direct URL**: type `/shortcuts` in URL bar → page loads, "Shortcuts" tab appears.
7. **Workspace switching**: create a new workspace via the workspace dropdown → navbar shows the new workspace's defaults (settings, workspace, lightroom). Open a different tab. Switch back to the original workspace → its tabs are preserved.
8. **Keyboard shortcut**: press `l` → navigates to `/lightroom`, tab appears if it was closed.

```
kill %1
```

**Step 3: Commit any final fixes**

If anything needed adjusting, commit per-task with focused messages.

---

### Task 15: Open the PR

**Step 1: Push and create the PR**

```bash
git push -u origin lightroom-import-relocate
gh pr create --base main --title "Openable navbar tabs" --body "$(cat <<'EOF'
## Summary
- Splits the navbar into always-shown **linger pages** and openable/closeable **tabs** (settings, workspace, lightroom, shortcuts, keywords, duplicates, logs).
- Tabs are persisted per workspace in a new `workspaces.open_tabs` JSON column, default `[settings, workspace, lightroom]`.
- A `+ Tools ▾` dropdown toggles tabs on/off; the canonical openable set is enforced server-side.
- Direct URL visits and keyboard shortcuts auto-open the corresponding tab so URLs and bookmarks keep working.
- Drag-to-reorder still works on linger pages; tabs are ordered by `open_tabs`.

Design: `docs/plans/2026-04-25-openable-tabs-design.md`
Plan: `docs/plans/2026-04-25-openable-tabs-plan.md`

## Test plan
- [x] `python -m pytest tests/test_workspaces.py vireo/tests/test_tabs_api.py -v`
- [x] Manual: default navbar, open/close from Tools menu, close-while-on-page redirect, direct URL auto-open, workspace switching preserves per-workspace tabs.
EOF
)"
```

---

## Notes for the implementer

- Vireo is a **single-user app** (see [memory: Solo-user app](memory)) — no historical migration matrix beyond the one ALTER in Task 1.
- **Plan docs are gitignored** (see [memory: Plan docs force-added](memory)) — both design and plan docs were committed with `git add -f`.
- The pr-agent system (see project CLAUDE.md) will review the PR on push. Push fixes to the same branch (see [memory: Review fixes same branch](memory)).
- If any UI behavior surprises you, **drive a real browser** to verify (see [memory: User-first testing](memory)) — don't infer from code alone.
