# Subject Keyword Types Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Generalize Vireo's "is this photo identified" predicate from species-only (`has_species`) to a configurable set of keyword *types* (`taxonomy`, `individual`, `place`, `scene`, `general`), so users can drop landscape/portrait/place photos out of the species-classification queue and skip the AI classifier on them.

**Architecture:** Reuse existing `keywords.type` column. Add a per-workspace config key `subject_types` (default `{taxonomy, individual, scene}`) stored in `workspaces.config_overrides`. Introduce a new `has_subject` collection rule and a classifier skip-gate that both read this set. Migrate the default "Needs Classification" collection to use the new rule. No schema changes.

**Tech Stack:** Python 3, Flask, SQLite, Jinja2, vanilla JS.

**Design doc:** `docs/plans/2026-04-25-subject-keyword-types-design.md`

**Working directory:** `/Users/julius/conductor/workspaces/vireo/missoula` (worktree on branch `unlabeled-query`).

**Testing baseline (run between tasks):**
```bash
python -m pytest tests/test_workspaces.py vireo/tests/test_db.py vireo/tests/test_app.py vireo/tests/test_photos_api.py vireo/tests/test_edits_api.py vireo/tests/test_jobs_api.py vireo/tests/test_darktable_api.py vireo/tests/test_config.py -v
```

**YAGNI deferrals from design** (noted, not blockers):
- Type-editing dropdown for existing keywords on the keywords page (only filter buttons updated in v1)
- Type picker in the inline-tag input on the lightbox (defer; the lightbox "Not Wildlife" button + autocomplete on the four shipped scene keywords cover the common case)

---

## Task 1: KEYWORD_TYPES constant + validation in `add_keyword`

**Files:**
- Modify: `vireo/db.py:3138` (add `kw_type` parameter to `add_keyword`)
- Modify: top of `vireo/db.py` (add module-level constant)
- Test: `vireo/tests/test_db.py`

**Step 1: Write the failing test**

Add to `vireo/tests/test_db.py`:

```python
def test_add_keyword_accepts_valid_types(temp_db):
    """add_keyword stores the requested type when it's a valid enum value."""
    db = temp_db
    db.set_active_workspace(db.create_workspace("ws"))
    kid = db.add_keyword("Charlie", kw_type="individual")
    row = db.conn.execute("SELECT type FROM keywords WHERE id = ?", (kid,)).fetchone()
    assert row["type"] == "individual"


def test_add_keyword_rejects_unknown_type(temp_db):
    """add_keyword raises ValueError for unknown type values."""
    import pytest
    db = temp_db
    db.set_active_workspace(db.create_workspace("ws"))
    with pytest.raises(ValueError, match="invalid keyword type"):
        db.add_keyword("BadType", kw_type="alien")


def test_keyword_types_constant():
    """KEYWORD_TYPES contains exactly the five valid enum values."""
    from vireo.db import KEYWORD_TYPES
    assert KEYWORD_TYPES == frozenset({"taxonomy", "individual", "place", "scene", "general"})
```

**Step 2: Run test to verify it fails**

```bash
python -m pytest vireo/tests/test_db.py::test_keyword_types_constant -v
```

Expected: FAIL with `ImportError: cannot import name 'KEYWORD_TYPES'` (or similar).

**Step 3: Add the constant**

Near the top of `vireo/db.py` (after imports, before class definitions):

```python
KEYWORD_TYPES = frozenset({"taxonomy", "individual", "place", "scene", "general"})
SUBJECT_TYPES_DEFAULT = frozenset({"taxonomy", "individual", "scene"})
```

**Step 4: Update `add_keyword` signature and validation**

In `vireo/db.py:3138`, change:

```python
def add_keyword(self, name, parent_id=None, is_species=False, _commit=True):
```

to:

```python
def add_keyword(self, name, parent_id=None, is_species=False, kw_type=None, _commit=True):
    if kw_type is not None and kw_type not in KEYWORD_TYPES:
        raise ValueError(f"invalid keyword type: {kw_type!r}")
```

Then update the type-resolution block (`vireo/db.py:3186-3208`) to honor the explicit `kw_type` argument when provided, falling back to the existing auto-detection only when `kw_type is None`. Concretely, replace:

```python
        # Auto-detect taxonomy type from taxa table
        kw_type = 'general'
        taxon_id = None
        if is_species:
            kw_type = 'taxonomy'
        else:
            ...
```

with:

```python
        taxon_id = None
        if kw_type is None:
            # Auto-detect taxonomy type from taxa table
            kw_type = 'general'
            if is_species:
                kw_type = 'taxonomy'
            else:
                ...  # existing taxa lookup, keep unchanged
```

(Wrap the existing taxa-lookup block in the new `if kw_type is None:` branch.)

**Step 5: Run all three new tests**

```bash
python -m pytest vireo/tests/test_db.py::test_keyword_types_constant vireo/tests/test_db.py::test_add_keyword_accepts_valid_types vireo/tests/test_db.py::test_add_keyword_rejects_unknown_type -v
```

Expected: 3 PASS.

**Step 6: Run full test suite to confirm no regressions**

```bash
python -m pytest tests/test_workspaces.py vireo/tests/test_db.py vireo/tests/test_app.py vireo/tests/test_photos_api.py vireo/tests/test_edits_api.py vireo/tests/test_jobs_api.py -v
```

Expected: all PASS (modulo the pre-existing failures noted in MEMORY.md).

**Step 7: Commit**

```bash
git add vireo/db.py vireo/tests/test_db.py
git commit -m "feat(keywords): add KEYWORD_TYPES enum and validation in add_keyword

Introduce KEYWORD_TYPES = {taxonomy, individual, place, scene, general}
and SUBJECT_TYPES_DEFAULT = {taxonomy, individual, scene}. add_keyword
now accepts an optional kw_type override and validates it. Existing
auto-detection (taxonomy lookup via taxa table) only runs when no
explicit type is passed."
```

---

## Task 2: Default `subject_types` in global config

**Files:**
- Modify: `vireo/config.py`
- Test: `vireo/tests/test_config.py`

**Step 1: Inspect current config defaults**

Read `vireo/config.py` to find the existing `_DEFAULTS` (or equivalent) dict. Add `subject_types` alongside other top-level keys.

**Step 2: Write the failing test**

In `vireo/tests/test_config.py`:

```python
def test_default_subject_types_includes_taxonomy_individual_scene(tmp_path, monkeypatch):
    import config as cfg
    monkeypatch.setattr(cfg, "CONFIG_PATH", str(tmp_path / "config.json"))
    loaded = cfg.load()
    assert set(loaded.get("subject_types", [])) == {"taxonomy", "individual", "scene"}
```

**Step 3: Run test to verify it fails**

```bash
python -m pytest vireo/tests/test_config.py::test_default_subject_types_includes_taxonomy_individual_scene -v
```

Expected: FAIL (key missing).

**Step 4: Add the default**

In `vireo/config.py`, add to the defaults dict:

```python
"subject_types": ["taxonomy", "individual", "scene"],
```

**Step 5: Run test to verify it passes**

```bash
python -m pytest vireo/tests/test_config.py::test_default_subject_types_includes_taxonomy_individual_scene -v
```

Expected: PASS.

**Step 6: Commit**

```bash
git add vireo/config.py vireo/tests/test_config.py
git commit -m "feat(config): add default subject_types config key

Defaults to [taxonomy, individual, scene] — the keyword types that
count as 'identifying' a photo for queue/classifier purposes."
```

---

## Task 3: `db.get_subject_types()` helper

**Files:**
- Modify: `vireo/db.py` (add method on `Database`)
- Test: `vireo/tests/test_db.py`

**Step 1: Write the failing tests**

```python
def test_get_subject_types_returns_default(temp_db):
    db = temp_db
    db.set_active_workspace(db.create_workspace("ws"))
    assert db.get_subject_types() == {"taxonomy", "individual", "scene"}


def test_get_subject_types_honors_workspace_override(temp_db):
    db = temp_db
    ws_id = db.create_workspace("ws")
    db.set_active_workspace(ws_id)
    db.update_workspace_config(ws_id, {"subject_types": ["taxonomy"]})
    assert db.get_subject_types() == {"taxonomy"}


def test_get_subject_types_drops_unknown_values(temp_db):
    db = temp_db
    ws_id = db.create_workspace("ws")
    db.set_active_workspace(ws_id)
    db.update_workspace_config(ws_id, {"subject_types": ["taxonomy", "alien"]})
    assert db.get_subject_types() == {"taxonomy"}
```

(If `update_workspace_config` doesn't exist as-named, find the equivalent setter via `grep -n "config_overrides" vireo/db.py` and use it.)

**Step 2: Run tests**

```bash
python -m pytest vireo/tests/test_db.py::test_get_subject_types_returns_default vireo/tests/test_db.py::test_get_subject_types_honors_workspace_override vireo/tests/test_db.py::test_get_subject_types_drops_unknown_values -v
```

Expected: FAIL (method doesn't exist).

**Step 3: Implement**

Add to `Database` class in `vireo/db.py` (near other config-reading helpers — search for `get_effective_config`):

```python
def get_subject_types(self) -> set[str]:
    """Return the keyword types that count as 'identified' for the active workspace."""
    import config as cfg
    effective = self.get_effective_config(cfg.load())
    raw = effective.get("subject_types", list(SUBJECT_TYPES_DEFAULT))
    if not isinstance(raw, list):
        return set(SUBJECT_TYPES_DEFAULT)
    return {t for t in raw if t in KEYWORD_TYPES}
```

**Step 4: Run tests to verify pass**

```bash
python -m pytest vireo/tests/test_db.py -k "get_subject_types" -v
```

Expected: 3 PASS.

**Step 5: Commit**

```bash
git add vireo/db.py vireo/tests/test_db.py
git commit -m "feat(db): add get_subject_types() reading workspace config

Returns the set of keyword types that count as 'identifying' a photo,
sourced from the workspace's config_overrides with the global default
as fallback. Unknown type strings are dropped at read time."
```

---

## Task 4: `db.filter_out_subject_tagged()` helper

**Files:**
- Modify: `vireo/db.py`
- Test: `vireo/tests/test_db.py`

**Step 1: Write the failing test**

```python
def test_filter_out_subject_tagged_excludes_tagged_photos(temp_db):
    """filter_out_subject_tagged drops photos that have any keyword whose
    type is in the supplied set."""
    db = temp_db
    db.set_active_workspace(db.create_workspace("ws"))

    # Two photos: p1 has a 'scene' keyword, p2 has a 'general' keyword
    # only. Filtering with {scene} should keep p2 but drop p1.
    p1 = _make_photo(db, "p1.jpg")  # use existing test helper
    p2 = _make_photo(db, "p2.jpg")
    scene_kid = db.add_keyword("Landscape", kw_type="scene")
    gen_kid = db.add_keyword("note", kw_type="general")
    db.tag_photo(p1, scene_kid)
    db.tag_photo(p2, gen_kid)

    kept = db.filter_out_subject_tagged([p1, p2], {"scene"})
    assert kept == [p2]


def test_filter_out_subject_tagged_empty_set_returns_all(temp_db):
    db = temp_db
    db.set_active_workspace(db.create_workspace("ws"))
    p1 = _make_photo(db, "p1.jpg")
    assert db.filter_out_subject_tagged([p1], set()) == [p1]
```

(If `_make_photo` doesn't exist, use the pattern from existing `test_db.py` tests for inserting a photo. `grep -n "INSERT INTO photos" vireo/tests/test_db.py` to find one.)

**Step 2: Run tests to verify fail**

```bash
python -m pytest vireo/tests/test_db.py -k "filter_out_subject_tagged" -v
```

Expected: FAIL (`AttributeError`).

**Step 3: Implement**

Add to `Database`:

```python
def filter_out_subject_tagged(self, photo_ids, subject_types):
    """Return the subset of photo_ids whose photos do NOT have any keyword
    of a type in subject_types. Empty subject_types returns photo_ids unchanged."""
    if not subject_types or not photo_ids:
        return list(photo_ids)
    types = [t for t in subject_types if t in KEYWORD_TYPES]
    if not types:
        return list(photo_ids)
    pid_placeholders = ",".join("?" * len(photo_ids))
    type_placeholders = ",".join("?" * len(types))
    rows = self.conn.execute(
        f"""SELECT id FROM photos
            WHERE id IN ({pid_placeholders})
              AND id NOT IN (
                SELECT pk.photo_id FROM photo_keywords pk
                JOIN keywords k ON k.id = pk.keyword_id
                WHERE k.type IN ({type_placeholders})
              )""",
        list(photo_ids) + types,
    ).fetchall()
    kept_set = {r["id"] for r in rows}
    # Preserve original order
    return [pid for pid in photo_ids if pid in kept_set]
```

**Step 4: Run tests**

```bash
python -m pytest vireo/tests/test_db.py -k "filter_out_subject_tagged" -v
```

Expected: 2 PASS.

**Step 5: Commit**

```bash
git add vireo/db.py vireo/tests/test_db.py
git commit -m "feat(db): add filter_out_subject_tagged batch helper

Drops photos with any keyword of a configured 'subject' type. Used by
the classifier skip-gate in a follow-up task."
```

---

## Task 5: `has_subject` collection rule

**Files:**
- Modify: `vireo/db.py:5414-5557` (rules engine in `_build_collection_query`)
- Test: `vireo/tests/test_db.py`

**Step 1: Write failing tests**

```python
def test_has_subject_rule_matches_photos_without_subject_keywords(temp_db):
    """A collection with has_subject==0 returns photos that have no
    keyword of type in the workspace's subject_types set."""
    db = temp_db
    ws_id = db.create_workspace("ws")
    db.set_active_workspace(ws_id)
    folder_id = _make_folder(db)  # helper from existing tests
    p1 = _make_photo_in_folder(db, "p1.jpg", folder_id)
    p2 = _make_photo_in_folder(db, "p2.jpg", folder_id)
    db.add_workspace_folder(ws_id, folder_id)

    scene_kid = db.add_keyword("Landscape", kw_type="scene")
    db.tag_photo(p1, scene_kid)  # p1 is identified, p2 is not

    cid = db.add_collection(
        "Needs Subject",
        json.dumps([{"field": "has_subject", "op": "equals", "value": 0}]),
    )
    photos = db.get_collection_photos(cid, per_page=999)
    pids = {p["id"] for p in photos}
    assert pids == {p2}


def test_has_subject_rule_with_value_one_matches_identified(temp_db):
    """has_subject==1 returns the inverse — photos WITH a subject keyword."""
    # Mirror of above; assert pids == {p1}
    ...


def test_has_subject_rule_empty_subject_types_no_match_for_value_one(temp_db):
    """When subject_types is empty, has_subject==1 should match no photos."""
    db = temp_db
    ws_id = db.create_workspace("ws")
    db.set_active_workspace(ws_id)
    db.update_workspace_config(ws_id, {"subject_types": []})
    folder_id = _make_folder(db)
    p1 = _make_photo_in_folder(db, "p1.jpg", folder_id)
    db.add_workspace_folder(ws_id, folder_id)
    cid = db.add_collection(
        "Has Subject",
        json.dumps([{"field": "has_subject", "op": "equals", "value": 1}]),
    )
    photos = db.get_collection_photos(cid, per_page=999)
    assert photos == []
```

**Step 2: Run tests to verify fail**

Expected: FAIL (rule field unhandled, returns all photos).

**Step 3: Implement the rule branch**

In `vireo/db.py`, inside `_build_collection_query`, add a branch alongside `has_species` (around line 5510):

```python
            elif field == "has_subject":
                subject_types = list(self.get_subject_types())
                if not subject_types:
                    # Empty set: no keyword type counts as "identifying"
                    if op == "equals" and (value is True or value == 1):
                        conditions.append("0")  # nothing matches
                    # value==0 is a no-op (every photo is "not identified")
                    continue
                type_placeholders = ",".join("?" * len(subject_types))
                if op == "equals" and (value is False or value == 0):
                    conditions.append(
                        f"""NOT EXISTS (
                        SELECT 1 FROM photo_keywords pk5
                        JOIN keywords k5 ON k5.id = pk5.keyword_id
                        WHERE pk5.photo_id = p.id AND k5.type IN ({type_placeholders}))"""
                    )
                    params.extend(subject_types)
                elif op == "equals" and (value is True or value == 1):
                    conditions.append(
                        f"""EXISTS (
                        SELECT 1 FROM photo_keywords pk5
                        JOIN keywords k5 ON k5.id = pk5.keyword_id
                        WHERE pk5.photo_id = p.id AND k5.type IN ({type_placeholders}))"""
                    )
                    params.extend(subject_types)
```

**Step 4: Run tests**

Expected: 3 PASS.

**Step 5: Commit**

```bash
git add vireo/db.py vireo/tests/test_db.py
git commit -m "feat(collections): add has_subject rule for the configured type set

Mirrors has_species but reads the workspace's subject_types config so
users can include scene/individual/place keywords in the 'identified'
predicate. has_species is preserved for narrower queries."
```

---

## Task 6: `PUT /api/workspaces/<id>/subject-types` endpoint

**Files:**
- Modify: `vireo/app.py` (add route)
- Test: `vireo/tests/test_app.py`

**Step 1: Write the failing test**

```python
def test_put_subject_types_persists_valid_values(client, db):
    ws_id = db.create_workspace("ws")
    resp = client.put(
        f"/api/workspaces/{ws_id}/subject-types",
        json={"types": ["taxonomy", "scene"]},
    )
    assert resp.status_code == 200
    assert set(db.get_workspace_config_overrides(ws_id).get("subject_types", [])) == {"taxonomy", "scene"}


def test_put_subject_types_drops_unknown(client, db):
    ws_id = db.create_workspace("ws")
    resp = client.put(
        f"/api/workspaces/{ws_id}/subject-types",
        json={"types": ["taxonomy", "bogus"]},
    )
    assert resp.status_code == 200
    assert db.get_workspace_config_overrides(ws_id).get("subject_types") == ["taxonomy"]


def test_put_subject_types_empty_list_allowed(client, db):
    ws_id = db.create_workspace("ws")
    resp = client.put(
        f"/api/workspaces/{ws_id}/subject-types",
        json={"types": []},
    )
    assert resp.status_code == 200
```

(Use the test fixtures already in `vireo/tests/test_app.py`. If `get_workspace_config_overrides` isn't a real method, use whatever existing helper reads `workspaces.config_overrides` — `grep -n "config_overrides" vireo/db.py`.)

**Step 2: Run tests to verify fail (404)**

**Step 3: Add the route**

In `vireo/app.py`, near other workspace routes (search for `@app.route("/api/workspaces`):

```python
@app.route("/api/workspaces/<int:ws_id>/subject-types", methods=["PUT"])
def api_set_subject_types(ws_id):
    body = request.get_json() or {}
    raw_types = body.get("types", [])
    if not isinstance(raw_types, list):
        return jsonify({"error": "types must be a list"}), 400
    from vireo.db import KEYWORD_TYPES
    cleaned = [t for t in raw_types if t in KEYWORD_TYPES]
    dropped = set(raw_types) - set(cleaned)
    if dropped:
        log.warning("subject-types: dropped unknown values %s", sorted(dropped))
    if not cleaned:
        log.warning("subject-types: empty list set for workspace %d", ws_id)
    db.update_workspace_config_key(ws_id, "subject_types", cleaned)
    return jsonify({"types": cleaned})
```

(Add `update_workspace_config_key(ws_id, key, value)` to `Database` if no equivalent exists — small wrapper around the JSON-merge into `workspaces.config_overrides`. Add a parallel test for that helper.)

**Step 4: Run tests**

Expected: 3 PASS.

**Step 5: Commit**

```bash
git add vireo/app.py vireo/db.py vireo/tests/test_app.py vireo/tests/test_db.py
git commit -m "feat(api): PUT /api/workspaces/<id>/subject-types

Lets clients configure which keyword types count as 'identifying' for
this workspace. Unknown values silently dropped (logged); empty list
allowed (logged warning)."
```

---

## Task 7: Migrate the default "Needs Classification" collection

**Files:**
- Modify: `vireo/db.py:5706` (`create_default_collections`)
- Modify: `vireo/db.py` (add a one-shot migration in `Database.__init__` or a numbered migration)
- Test: `vireo/tests/test_db.py`

**Step 1: Write failing tests**

```python
def test_default_collection_uses_has_subject_for_new_workspaces(temp_db):
    db = temp_db
    ws_id = db.create_workspace("ws")
    db.set_active_workspace(ws_id)
    cols = {c["name"]: json.loads(c["rules"]) for c in db.get_collections()}
    assert "Needs Identification" in cols
    assert cols["Needs Identification"] == [{"field": "has_subject", "op": "equals", "value": 0}]


def test_existing_needs_classification_collection_renamed_idempotently(temp_db):
    """A workspace pre-populated with the old default gets renamed; running
    again is a no-op."""
    db = temp_db
    ws_id = db.create_workspace("ws")
    db.set_active_workspace(ws_id)
    # Force-create the legacy state
    db.add_collection(
        "Needs Classification",
        json.dumps([{"field": "has_species", "op": "equals", "value": 0}]),
    )
    db.migrate_default_subject_collection()
    db.migrate_default_subject_collection()  # idempotent
    cols = {c["name"]: json.loads(c["rules"]) for c in db.get_collections()}
    assert "Needs Identification" in cols
    assert "Needs Classification" not in cols
    assert cols["Needs Identification"] == [{"field": "has_subject", "op": "equals", "value": 0}]


def test_migration_skips_user_customized_collection(temp_db):
    """If 'Needs Classification' has a non-default rule (user edited it),
    leave it alone."""
    db = temp_db
    ws_id = db.create_workspace("ws")
    db.set_active_workspace(ws_id)
    custom = [{"field": "rating", "op": ">=", "value": 3}]
    db.add_collection("Needs Classification", json.dumps(custom))
    db.migrate_default_subject_collection()
    cols = {c["name"]: json.loads(c["rules"]) for c in db.get_collections()}
    assert "Needs Classification" in cols
    assert cols["Needs Classification"] == custom
```

**Step 2: Run tests to verify fail**

**Step 3: Update `create_default_collections` and add migration**

In `vireo/db.py:5710-5715`, change the second tuple from:

```python
("Needs Classification", [{"field": "has_species", "op": "equals", "value": 0}]),
```

to:

```python
("Needs Identification", [{"field": "has_subject", "op": "equals", "value": 0}]),
```

Add a new method:

```python
def migrate_default_subject_collection(self):
    """Rename 'Needs Classification' → 'Needs Identification' and switch
    its rule to has_subject==0, but only if it still matches the prior
    default rule (so user customizations are preserved). Idempotent."""
    rows = self.conn.execute(
        "SELECT id, name, rules FROM collections WHERE workspace_id = ? AND name = ?",
        (self._ws_id(), "Needs Classification"),
    ).fetchall()
    legacy_rule = [{"field": "has_species", "op": "equals", "value": 0}]
    for row in rows:
        try:
            current = json.loads(row["rules"])
        except (TypeError, ValueError):
            continue
        if current != legacy_rule:
            continue
        # Don't clobber an existing "Needs Identification"
        existing = self.conn.execute(
            "SELECT 1 FROM collections WHERE workspace_id = ? AND name = ?",
            (self._ws_id(), "Needs Identification"),
        ).fetchone()
        if existing:
            continue
        self.conn.execute(
            "UPDATE collections SET name = ?, rules = ? WHERE id = ?",
            (
                "Needs Identification",
                json.dumps([{"field": "has_subject", "op": "equals", "value": 0}]),
                row["id"],
            ),
        )
    self.conn.commit()
```

Call `migrate_default_subject_collection()` after `create_default_collections()` in workspace setup (find the existing call site of `create_default_collections` via `grep -n "create_default_collections" vireo/db.py`).

**Step 4: Run tests**

Expected: 3 PASS.

**Step 5: Commit**

```bash
git add vireo/db.py vireo/tests/test_db.py
git commit -m "feat(collections): migrate default to Needs Identification + has_subject

New workspaces get 'Needs Identification' (rule has_subject==0). Existing
workspaces with the legacy 'Needs Classification' default are renamed
in place; user-customized collections are left untouched. Idempotent."
```

---

## Task 8: Default `scene` keywords

**Files:**
- Modify: `vireo/db.py` (add to `Database.__init__` or workspace-setup path)
- Test: `vireo/tests/test_db.py`

**Step 1: Write the failing test**

```python
def test_default_scene_keywords_inserted(temp_db):
    db = temp_db
    db.set_active_workspace(db.create_workspace("ws"))
    rows = db.conn.execute(
        "SELECT name FROM keywords WHERE type = 'scene' ORDER BY name"
    ).fetchall()
    assert [r["name"] for r in rows] == ["Abstract", "Architecture", "Landscape", "Sunset"]


def test_default_scene_keywords_idempotent(temp_db):
    db = temp_db
    db.set_active_workspace(db.create_workspace("ws"))
    db.ensure_default_scene_keywords()  # idempotent
    rows = db.conn.execute(
        "SELECT COUNT(*) AS n FROM keywords WHERE type = 'scene'"
    ).fetchone()
    assert rows["n"] == 4
```

**Step 2: Run tests to verify fail**

**Step 3: Add the method and call site**

```python
def ensure_default_scene_keywords(self):
    """Insert the default scene keywords if none exist of type='scene'.
    Idempotent."""
    existing = self.conn.execute(
        "SELECT 1 FROM keywords WHERE type = 'scene' LIMIT 1"
    ).fetchone()
    if existing:
        return
    for name in ("Landscape", "Sunset", "Architecture", "Abstract"):
        self.conn.execute(
            "INSERT OR IGNORE INTO keywords (name, type, is_species) VALUES (?, 'scene', 0)",
            (name,),
        )
    self.conn.commit()
```

Call from `Database.__init__` (after schema creation, before `create_default_collections`).

**Step 4: Run tests**

Expected: 2 PASS.

**Step 5: Commit**

```bash
git add vireo/db.py vireo/tests/test_db.py
git commit -m "feat(keywords): ship default scene keywords

Landscape, Sunset, Architecture, Abstract — created once on first DB
init, idempotent thereafter. Enables the lightbox 'Not Wildlife' button
and inline-tag autocomplete for the common landscape case."
```

---

## Task 9: Classifier skip-gate

**Files:**
- Modify: `vireo/classify_job.py:1369` (post-load filtering of photos)
- Test: `vireo/tests/test_jobs_api.py` or a new dedicated test file

**Step 1: Read the relevant block** at `classify_job.py:1369-1480` to confirm the photo-list flows through `photos = thread_db.get_collection_photos(...)` and then `photo_ids` later. Identify the right insertion point — after the load step, before model load (so we can short-circuit total to zero if everything is filtered).

**Step 2: Write failing test**

```python
def test_classify_job_skips_photos_with_subject_keywords(monkeypatch, tmp_path, ...):
    """When a photo has a keyword whose type is in subject_types, the
    classifier doesn't include it in the run."""
    # Set up: workspace, two photos in the collection. Tag p1 with a
    # 'scene' keyword. Run classify_job with a stubbed classifier that
    # records which photo IDs got passed in. Assert only p2 was processed.
    ...


def test_classify_job_reclassify_true_bypasses_subject_skip(...):
    """With reclassify=True, even subject-tagged photos are reprocessed."""
    ...
```

(Mirror the harness from existing tests in `vireo/tests/test_jobs_api.py`. The test stubs the classifier model so we don't actually load weights.)

**Step 3: Run tests to verify fail**

**Step 4: Implement**

After `photos = thread_db.get_collection_photos(...)` at line 1369, insert:

```python
        if not params.reclassify:
            subject_types = thread_db.get_subject_types()
            if subject_types:
                pre_count = len(photos)
                kept_ids = set(thread_db.filter_out_subject_tagged(
                    [p["id"] for p in photos], subject_types,
                ))
                photos = [p for p in photos if p["id"] in kept_ids]
                skipped_subject = pre_count - len(photos)
                if skipped_subject:
                    log.info(
                        "Skipping %d photo(s) with subject keywords (types=%s)",
                        skipped_subject, sorted(subject_types),
                    )
                    runner.push_event(
                        job["id"], "progress",
                        {
                            "current": 0,
                            "total": len(photos),
                            "current_file": (
                                f"Skipped {skipped_subject} already-identified "
                                f"photo(s)"
                            ),
                            "rate": 0,
                            "phase": "Step 2/5: Loading photos",
                            "skipped_subject": skipped_subject,
                        },
                    )
```

**Step 5: Run tests**

Expected: 2 PASS.

**Step 6: Run job-related tests for regressions**

```bash
python -m pytest vireo/tests/test_jobs_api.py -v
```

**Step 7: Commit**

```bash
git add vireo/classify_job.py vireo/tests/test_jobs_api.py
git commit -m "feat(classify): skip photos already tagged with a subject keyword

Default behavior: classify job filters out photos whose keywords
include any type in the workspace's subject_types. reclassify=True
bypasses, allowing explicit re-verification. Skip count is surfaced
in progress events."
```

---

## Task 10: Keywords page filter-button rename

**Files:**
- Modify: `vireo/templates/keywords.html:162-164`
- Test: `vireo/testing/userfirst/scenarios/keywords.py` (existing scenario uses `data-type="taxonomy"` — should still work; add a new assertion if useful)

**Step 1: Update the markup**

In `keywords.html`, replace:

```html
<button class="kw-filter-btn" data-type="general">General</button>
<button class="kw-filter-btn" data-type="taxonomy">Taxonomy</button>
<button class="kw-filter-btn" data-type="location">Location</button>
```

with:

```html
<button class="kw-filter-btn" data-type="general">General</button>
<button class="kw-filter-btn" data-type="taxonomy">Taxonomy</button>
<button class="kw-filter-btn" data-type="individual">Individual</button>
<button class="kw-filter-btn" data-type="place">Place</button>
<button class="kw-filter-btn" data-type="scene">Scene</button>
```

(Search the file for any JS handler that switches on the `data-type` value to make sure the new ones flow through correctly. The handler likely just compares to `keywords.type` strings, which is exactly what we need.)

**Step 2: Quick smoke check**

```bash
python -m pytest vireo/testing/userfirst/scenarios/keywords.py -v
```

Expected: PASS (existing scenario uses `data-type="taxonomy"` which is unchanged).

**Step 3: Commit**

```bash
git add vireo/templates/keywords.html
git commit -m "ui(keywords): expand filter buttons to full type enum

general | taxonomy | individual | place | scene. Replaces the
previously-stub 'location' button with 'place' to match the canonical
type name."
```

---

## Task 11: Lightbox "Not Wildlife" button

**Files:**
- Modify: `vireo/templates/_navbar.html` or wherever the lightbox markup lives (search via `grep -rn "lightbox" vireo/templates/`)
- Modify: corresponding JS handler
- Test: write a Playwright/userfirst scenario or a manual test plan

**Step 1: Locate the lightbox controls**

```bash
grep -n "accept\|reject\|species" vireo/templates/_navbar.html | head -30
```

Find the row of controls near the species accept/reject buttons.

**Step 2: Add the button (HTML)**

Adjacent to the species accept/reject controls, add:

```html
<button id="not-wildlife-btn" class="lightbox-action-btn" title="Mark as not wildlife (Landscape)">
  Not Wildlife
</button>
```

**Step 3: Add the JS handler**

```javascript
document.getElementById("not-wildlife-btn")?.addEventListener("click", async () => {
  const photoId = currentLightboxPhotoId;  // use whatever existing variable holds this
  if (!photoId) return;
  const resp = await fetch(`/api/photos/${photoId}/keywords`, {
    method: "POST",
    headers: {"Content-Type": "application/json"},
    body: JSON.stringify({name: "Landscape"}),
  });
  if (resp.ok) {
    advanceLightbox();  // existing function
  } else {
    console.error("Failed to tag as Landscape", await resp.text());
  }
});
```

(The exact name of the keyword-tagging endpoint and the advance function should be confirmed by `grep -n "/api/photos.*keywords" vireo/app.py` and `grep -n "advance" vireo/templates/_navbar.html`.)

**Step 4: Verify the endpoint accepts a name and resolves to the existing scene keyword**

The "Landscape" keyword was inserted in Task 8. The tagging endpoint should look up by name (case-insensitive) and reuse the existing keyword rather than creating a new `general` one. If the endpoint creates a new keyword via `add_keyword`, double-check it doesn't override the existing scene-typed one. (`add_keyword` already returns the existing ID for case-insensitive matches — `db.py:3152-3171` — so reuse is automatic.)

**Step 5: Manual smoke test**

```bash
python vireo/app.py --db /tmp/vireo-test.db --port 8080
```

Open http://localhost:8080, tag a photo via the new button, confirm:
- The Landscape keyword appears on the photo
- The photo no longer appears in "Needs Identification"
- The lightbox advances to the next photo

**Step 6: Commit**

```bash
git add vireo/templates/_navbar.html
git commit -m "ui(lightbox): add 'Not Wildlife' quick-tag button

Tags the current photo with the default 'Landscape' scene keyword and
advances. Drops the photo from 'Needs Identification' immediately."
```

---

## Task 12: Workspace settings panel — subject-types section

**Files:**
- Modify: workspace-settings template (find via `grep -rn "workspace.*settings\|settings.*workspace" vireo/templates/`)
- Test: write a scenario or manual test

**Step 1: Locate the settings template** and identify a good place for a new "What counts as identified?" section.

**Step 2: Add the form section**

```html
<section class="settings-section">
  <h3>What counts as identified?</h3>
  <p class="hint">Photos with at least one keyword of these types drop out of "Needs Identification" and are skipped by the classifier.</p>
  <label><input type="checkbox" name="subject-type" value="taxonomy" checked> Taxonomy (species)</label>
  <label><input type="checkbox" name="subject-type" value="individual" checked> Individual (people, pets)</label>
  <label><input type="checkbox" name="subject-type" value="place"> Place</label>
  <label><input type="checkbox" name="subject-type" value="scene" checked> Scene (landscape, sunset, etc.)</label>
  <label><input type="checkbox" name="subject-type" value="general"> General</label>
  <button id="save-subject-types">Save</button>
</section>
```

**Step 3: Wire up the JS** to:
- On page load: GET the current workspace config, set checkbox state from `subject_types`
- On Save: PUT to `/api/workspaces/<id>/subject-types` with the checked values

**Step 4: Manual smoke test** — change the set, reload page, confirm persistence; flip a workspace to `{taxonomy}` only and confirm a `scene=Landscape` photo reappears in Needs Identification.

**Step 5: Commit**

```bash
git add vireo/templates/<settings-template>.html
git commit -m "ui(settings): add subject-types configuration section

Per-workspace checkbox group lets users pick which keyword types count
as 'identifying' a photo for queue/classifier purposes."
```

---

## Task 13: Pipeline page tooltip update

**Files:**
- Modify: `vireo/templates/pipeline.html` (find the Re-classify control via `grep -n -i "reclassify\|re-classify" vireo/templates/pipeline.html`)

**Step 1: Update the tooltip/help text**

Change the existing tooltip from something like "Re-classify already-classified photos" to:

> "Re-classify already-classified photos. Also bypasses the skip for photos already tagged with a subject keyword (e.g. Landscape) — useful for double-checking existing tags."

**Step 2: Commit**

```bash
git add vireo/templates/pipeline.html
git commit -m "ui(pipeline): clarify re-classify tooltip mentions subject-tag bypass"
```

---

## Verification & PR

**Final test run:**

```bash
python -m pytest tests/test_workspaces.py vireo/tests/test_db.py vireo/tests/test_app.py vireo/tests/test_photos_api.py vireo/tests/test_edits_api.py vireo/tests/test_jobs_api.py vireo/tests/test_darktable_api.py vireo/tests/test_config.py -v
```

Expected: all PASS (modulo MEMORY-noted pre-existing failures).

**Manual end-to-end scenario:**

1. Start with a fresh DB.
2. Open browser, confirm "Needs Identification" appears in Collections (not "Needs Classification").
3. Run a small classify on a folder of mixed photos.
4. In the lightbox, click "Not Wildlife" on a landscape photo. Confirm it disappears from Needs Identification.
5. In Workspace Settings, uncheck "Scene" — that landscape photo reappears in Needs Identification.
6. Re-check "Scene", confirm it disappears again.
7. Run classify a second time WITHOUT reclassify — the landscape photo is not re-processed (skip count appears in the bottom panel).
8. Run classify with reclassify=True — the landscape photo IS re-processed.

**Create the PR:**

```bash
git push -u origin unlabeled-query
gh pr create --base main --title "feat: subject keyword types — generalize 'is identified' beyond species" --body "$(cat <<'EOF'
## Summary
- Add fixed five-value enum for `keywords.type`: `taxonomy`, `individual`, `place`, `scene`, `general`
- New per-workspace `subject_types` config (default `{taxonomy, individual, scene}`) defines which types count as "identifying" a photo
- New `has_subject` collection rule; default "Needs Classification" → "Needs Identification" with the new rule
- Classifier skips photos with any subject-typed keyword (bypassed by `reclassify=True`)
- Lightbox "Not Wildlife" button → tags with default `Landscape` scene keyword
- Workspace settings: checkbox UI for the subject-types set
- Ship four default `scene` keywords: Landscape, Sunset, Architecture, Abstract

Design doc: `docs/plans/2026-04-25-subject-keyword-types-design.md`
Plan: `docs/plans/2026-04-25-subject-keyword-types-plan.md`

## Test plan
- [x] Unit tests for KEYWORD_TYPES, get_subject_types, filter_out_subject_tagged, has_subject rule
- [x] API tests for PUT /subject-types
- [x] Migration tests (rename idempotent, doesn't clobber customizations)
- [x] Classify-job skip + bypass tests
- [x] Manual end-to-end: Not Wildlife button + settings panel toggle

🤖 Generated with [Claude Code](https://claude.com/claude-code)
EOF
)"
```

---

## Out-of-scope follow-ups (post-merge)

- Type-editing dropdown for existing keywords on the keywords page (deferred YAGNI from design)
- Type picker in inline-tag input on the lightbox (deferred YAGNI)
- Optional `species_keyword_id` FK on individuals (only if individual-tracking workflows materialize)
