# Life List Explorer Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Add a taxonomic completeness explorer to `/life-list` that shows, for a chosen class (birds by default), how many orders/families/genera/species exist and how many the user has found, with drill-down cards + progress rings and a sunburst showpiece.

**Architecture:** A new DB method computes the class subtree from the local iNaturalist reference taxonomy (already in `taxa`, linked by `parent_id`) via a recursive CTE, then rolls up found-vs-total in Python from the workspace-scoped set of tagged species taxa. A new `/api/life-list/explorer` endpoint serves the tree + honesty states; a species-leaf endpoint serves found+missing species per genus. The `life_list.html` template gains a two-tab layout (List / Explorer); the Explorer tab renders class selector → sunburst → breadcrumb → card grid → species leaf, all vanilla JS + inline styles using existing theme vars.

**Tech Stack:** Flask, SQLite (recursive CTE), Jinja2, vanilla JS, inline SVG (sunburst). Tests via pytest (`vireo/tests/`).

**Design doc:** `docs/plans/2026-07-07-life-list-explorer-design.md`

---

## Background facts (verified in codebase)

- `taxa` table (`vireo/db.py:495`): `id, inat_id, name, common_name, rank, parent_id, kingdom`. Index `idx_taxa_parent ON taxa(parent_id)` exists (`db.py:822`). `rank` values are lowercase: `kingdom, phylum, class, order, family, genus, species` (`vireo/taxonomy.py:210`). The table holds the **complete** iNat taxonomy once downloaded, not just user-seen taxa.
- `keywords.taxon_id` (`db.py:505`) links a species keyword to its `taxa` row. `photo_keywords` links photos↔keywords.
- Existing life-list eligibility & workspace scope (MATCH THIS EXACTLY) — `get_life_list_candidates()` (`db.py:9077`):
  ```sql
  FROM photo_keywords pk
  JOIN keywords k ON k.id = pk.keyword_id AND (k.is_species = 1 OR k.type = 'taxonomy')
  JOIN photos p ON p.id = pk.photo_id
  JOIN workspace_folders wf ON wf.folder_id = p.folder_id AND wf.workspace_id = ?
  JOIN folders f ON f.id = p.folder_id AND f.status IN ('ok', 'partial')
  LEFT JOIN taxa t ON t.id = k.taxon_id
  WHERE COALESCE(p.flag, 'none') != 'rejected'
  ```
- `_ws_id()` (`db.py:1184`) returns the active workspace id (raises if unset).
- Life-list page route: `@app.route("/life-list")` → `life_list_page()` (`app.py:2539`). JSON: `@app.route("/api/life-list")` → `api_life_list()` (`app.py:6933`), building via `_build_life_list_payload()` (`app.py:6817`). All use `db = _get_db()`.
- Thumbnails served at `/thumbnails/<id>.jpg` (`app.py:16117`). Template helper `photoThumbnailUrl(photo)` (life_list.html:208).
- Taxonomy readiness already exposed at `/api/taxonomy/info` (`app.py:11591`) and `/api/classify/config` (`taxonomy_available`, `taxonomy_species_count`). Download job: `POST /api/jobs/download-taxonomy` (`app.py:11597`).
- Tabs pattern to copy: `vireo/templates/audit.html` (`.tabs`/`.tab`/`.tab-panel` CSS lines 41-81; `switchTab()` JS lines 180-187).
- Theme vars available (from `vireo/static/vireo-theme.css`): `--bg-primary/secondary/tertiary/input/panel`, `--border-primary/secondary/subtle`, `--text-primary/secondary/muted/dim`, `--accent`, `--accent-hover`, `--accent-text`.
- Test fixtures (`vireo/tests/conftest.py`): `db` fixture (line 107, temp-file Database), `app_and_db` fixture (line 115, seeded photos/keywords + test client, `api_token="test-token-123"`). Seed helpers: `db.add_folder`, `db.add_photo`, `db.add_keyword`, `db.tag_photo`, `db.ensure_default_workspace`, `db.set_active_workspace`.

---

## STAGE 1 — Backend: DB completeness engine + endpoints

### Task 1: DB helper — resolve default class + taxonomy readiness

**Files:**
- Modify: `vireo/db.py` (add methods near the other taxa/life-list methods, ~line 9116)
- Test: `vireo/tests/test_db.py`

**Step 1: Write the failing test**

Add a small taxa-seeding helper at the top of the test module (reused by later tasks). Insert a mini bird tree so tests don't need the real taxonomy:

```python
def _seed_bird_taxonomy(db):
    """Insert a tiny Aves subtree: class Aves > 2 orders > families > genera > species.
    Returns dict of name -> taxa id."""
    rows = [
        # (inat_id, name, common_name, rank, parent_name, kingdom)
        (3,     "Aves",           "Birds",         "class",   None,             "Animalia"),
        (7251,  "Passeriformes",  "Perching Birds","order",   "Aves",           "Animalia"),
        (67566, "Passerellidae",  "New World Sparrows","family","Passeriformes", "Animalia"),
        (9100,  "Melospiza",      None,            "genus",   "Passerellidae",  "Animalia"),
        (9101,  "Melospiza melodia","Song Sparrow","species", "Melospiza",      "Animalia"),
        (9102,  "Melospiza georgiana","Swamp Sparrow","species","Melospiza",    "Animalia"),
        (9200,  "Zonotrichia",    None,            "genus",   "Passerellidae",  "Animalia"),
        (9201,  "Zonotrichia albicollis","White-throated Sparrow","species","Zonotrichia","Animalia"),
        (4000,  "Anseriformes",   "Waterfowl",     "order",   "Aves",           "Animalia"),
        (4100,  "Anatidae",       "Ducks",         "family",  "Anseriformes",   "Animalia"),
        (4200,  "Anas",           None,            "genus",   "Anatidae",       "Animalia"),
        (4201,  "Anas platyrhynchos","Mallard",    "species", "Anas",           "Animalia"),
    ]
    ids = {}
    for inat_id, name, common, rank, parent, kingdom in rows:
        parent_id = ids.get(parent)
        cur = db.conn.execute(
            "INSERT INTO taxa (inat_id, name, common_name, rank, parent_id, kingdom)"
            " VALUES (?,?,?,?,?,?)",
            (inat_id, name, common, rank, parent_id, kingdom),
        )
        ids[name] = cur.lastrowid
    db.conn.commit()
    return ids


def test_get_default_explorer_root_finds_aves(db):
    assert db.get_explorer_root() is None  # no taxonomy yet
    ids = _seed_bird_taxonomy(db)
    root = db.get_explorer_root()
    assert root["id"] == ids["Aves"]
    assert root["name"] == "Aves"
    assert root["rank"] == "class"
```

**Step 2: Run to verify it fails**

Run: `python -m pytest vireo/tests/test_db.py::test_get_default_explorer_root_finds_aves -v`
Expected: FAIL (`AttributeError: 'Database' object has no attribute 'get_explorer_root'`).

**Step 3: Implement**

Add to `vireo/db.py`:

```python
def get_explorer_root(self, name="Aves", rank="class"):
    """Return {id,name,common_name,rank} for the default explorer root class,
    or None when the reference taxonomy has not been downloaded."""
    row = self.conn.execute(
        "SELECT id, name, common_name, rank FROM taxa"
        " WHERE name = ? AND rank = ? LIMIT 1",
        (name, rank),
    ).fetchone()
    return dict(row) if row else None
```

**Step 4: Run to verify it passes**

Run: `python -m pytest vireo/tests/test_db.py::test_get_default_explorer_root_finds_aves -v`
Expected: PASS.

**Step 5: Commit**

```bash
git add vireo/db.py vireo/tests/test_db.py
git commit -m "feat(life-list): resolve default explorer root class (Aves)"
```

---

### Task 2: DB — workspace-scoped found species taxa + unmatched species

**Files:**
- Modify: `vireo/db.py`
- Test: `vireo/tests/test_db.py`

**Step 1: Write the failing test**

```python
def test_life_list_taxon_ids_scope(db):
    ids = _seed_bird_taxonomy(db)
    ws = db.ensure_default_workspace(); db.set_active_workspace(ws)
    fid = db.add_folder('/p', name='p')
    p1 = db.add_photo(folder_id=fid, filename='a.jpg', extension='.jpg',
                      file_size=1, file_mtime=1.0, timestamp='2024-01-01T00:00:00')
    p2 = db.add_photo(folder_id=fid, filename='b.jpg', extension='.jpg',
                      file_size=1, file_mtime=2.0, timestamp='2024-01-02T00:00:00')
    # Matched species keyword (linked to Song Sparrow taxon)
    k1 = db.add_keyword('Song Sparrow'); db.tag_photo(p1, k1)
    db.conn.execute("UPDATE keywords SET is_species=1, taxon_id=? WHERE id=?",
                    (ids['Melospiza melodia'], k1)); db.conn.commit()
    # Unmatched species keyword (is_species but no taxon_id)
    k2 = db.add_keyword('Mystery Warbler'); db.tag_photo(p2, k2)
    db.conn.execute("UPDATE keywords SET is_species=1 WHERE id=?", (k2,)); db.conn.commit()

    found = db.get_life_list_taxon_ids()
    assert found == {ids['Melospiza melodia']}
    unmatched = db.get_life_list_unmatched_species()
    assert 'Mystery Warbler' in unmatched

def test_life_list_taxon_ids_excludes_rejected(db):
    ids = _seed_bird_taxonomy(db)
    ws = db.ensure_default_workspace(); db.set_active_workspace(ws)
    fid = db.add_folder('/p', name='p')
    p = db.add_photo(folder_id=fid, filename='a.jpg', extension='.jpg',
                     file_size=1, file_mtime=1.0)
    k = db.add_keyword('Song Sparrow'); db.tag_photo(p, k)
    db.conn.execute("UPDATE keywords SET is_species=1, taxon_id=? WHERE id=?",
                    (ids['Melospiza melodia'], k)); db.conn.commit()
    db.update_photo_flag(p, 'rejected')  # verify method name in db.py; else set p.flag directly
    assert db.get_life_list_taxon_ids() == set()
```

> NOTE for implementer: confirm the exact "reject a photo" helper name in `db.py` (search `flag`); if none, `db.conn.execute("UPDATE photos SET flag='rejected' WHERE id=?", (p,))`.

**Step 2: Run — expect FAIL** (`get_life_list_taxon_ids` missing).

**Step 3: Implement** — mirror `get_life_list_candidates` scoping exactly:

```python
def get_life_list_taxon_ids(self):
    """Distinct taxa ids of workspace-scoped tagged species (same eligibility
    as get_life_list_candidates), excluding species keywords with no taxon_id."""
    ws = self._ws_id()
    rows = self.conn.execute(
        """SELECT DISTINCT k.taxon_id AS tid
           FROM photo_keywords pk
           JOIN keywords k ON k.id = pk.keyword_id
            AND (k.is_species = 1 OR k.type = 'taxonomy')
           JOIN photos p ON p.id = pk.photo_id
           JOIN workspace_folders wf ON wf.folder_id = p.folder_id
            AND wf.workspace_id = ?
           JOIN folders f ON f.id = p.folder_id
            AND f.status IN ('ok', 'partial')
           WHERE COALESCE(p.flag, 'none') != 'rejected'
             AND k.taxon_id IS NOT NULL""",
        (ws,),
    ).fetchall()
    return {r["tid"] for r in rows}

def get_life_list_unmatched_species(self):
    """Names of workspace-scoped tagged species keywords with no taxon_id —
    surfaced honestly in the explorer as 'not counted'."""
    ws = self._ws_id()
    rows = self.conn.execute(
        """SELECT DISTINCT k.name AS name
           FROM photo_keywords pk
           JOIN keywords k ON k.id = pk.keyword_id
            AND (k.is_species = 1 OR k.type = 'taxonomy')
           JOIN photos p ON p.id = pk.photo_id
           JOIN workspace_folders wf ON wf.folder_id = p.folder_id
            AND wf.workspace_id = ?
           JOIN folders f ON f.id = p.folder_id
            AND f.status IN ('ok', 'partial')
           WHERE COALESCE(p.flag, 'none') != 'rejected'
             AND k.taxon_id IS NULL
           ORDER BY k.name""",
        (ws,),
    ).fetchall()
    return [r["name"] for r in rows]
```

**Step 4: Run — expect PASS.**

**Step 5: Commit**

```bash
git add vireo/db.py vireo/tests/test_db.py
git commit -m "feat(life-list): workspace-scoped found taxa + unmatched species queries"
```

---

### Task 3: DB — taxon subtree fetch (recursive CTE)

**Files:** Modify `vireo/db.py`; Test `vireo/tests/test_db.py`.

**Step 1: Failing test**

```python
def test_get_taxon_subtree(db):
    ids = _seed_bird_taxonomy(db)
    rows = db.get_taxon_subtree(ids['Aves'])
    by_name = {r['name']: r for r in rows}
    assert by_name['Aves']['rank'] == 'class'
    assert by_name['Melospiza melodia']['rank'] == 'species'
    # parent linkage preserved
    assert by_name['Passeriformes']['parent_id'] == ids['Aves']
    assert by_name['Melospiza']['parent_id'] == ids['Passerellidae']
    # subtree of a genus is just its species + itself
    sub = {r['name'] for r in db.get_taxon_subtree(ids['Melospiza'])}
    assert sub == {'Melospiza', 'Melospiza melodia', 'Melospiza georgiana'}
```

**Step 2: Run — expect FAIL.**

**Step 3: Implement** (downward CTE; `idx_taxa_parent` makes the join efficient):

```python
def get_taxon_subtree(self, root_id, max_depth=12):
    """All taxa in the subtree rooted at root_id (inclusive), as dict rows
    with id, name, common_name, rank, parent_id. Uses the parent_id index."""
    return [dict(r) for r in self.conn.execute(
        """WITH RECURSIVE subtree(id, name, common_name, rank, parent_id, depth) AS (
               SELECT id, name, common_name, rank, parent_id, 0
               FROM taxa WHERE id = ?
               UNION ALL
               SELECT t.id, t.name, t.common_name, t.rank, t.parent_id, s.depth + 1
               FROM taxa t JOIN subtree s ON t.parent_id = s.id
               WHERE s.depth < ?
           )
           SELECT id, name, common_name, rank, parent_id FROM subtree""",
        (root_id, max_depth),
    ).fetchall()]
```

**Step 4: Run — expect PASS.**

**Step 5: Commit** `feat(life-list): recursive taxon subtree fetch`.

---

### Task 4: DB — classes the user has tagged species in (for the selector)

**Files:** Modify `vireo/db.py`; Test `vireo/tests/test_db.py`.

**Step 1: Failing test**

```python
def test_get_classes_for_taxa(db):
    ids = _seed_bird_taxonomy(db)
    classes = db.get_classes_for_taxa({ids['Melospiza melodia']})
    assert [c['name'] for c in classes] == ['Aves']
    assert db.get_classes_for_taxa(set()) == []
```

**Step 2: Run — expect FAIL.**

**Step 3: Implement** (upward CTE to each species' class ancestor):

```python
def get_classes_for_taxa(self, taxon_ids):
    """Distinct class-rank ancestors of the given taxa, for the explorer's
    class selector. Returns [{id,name,common_name}] ordered by name."""
    ids = [t for t in taxon_ids if t is not None]
    if not ids:
        return []
    placeholders = ",".join("?" for _ in ids)
    rows = self.conn.execute(
        f"""WITH RECURSIVE up(id) AS (
                SELECT id FROM taxa WHERE id IN ({placeholders})
                UNION
                SELECT t.parent_id FROM taxa t JOIN up u ON t.id = u.id
                WHERE t.parent_id IS NOT NULL
            )
            SELECT DISTINCT t.id, t.name, t.common_name
            FROM up u JOIN taxa t ON t.id = u.id
            WHERE t.rank = 'class'
            ORDER BY COALESCE(t.common_name, t.name)""",
        ids,
    ).fetchall()
    return [dict(r) for r in rows]
```

**Step 4: Run — expect PASS.**

**Step 5: Commit** `feat(life-list): class ancestors for explorer selector`.

---

### Task 5: DB — best photo per found species taxon (for the leaf)

**Files:** Modify `vireo/db.py`; Test `vireo/tests/test_db.py`.

**Step 1: Failing test**

```python
def test_best_photo_by_taxon(db):
    ids = _seed_bird_taxonomy(db)
    ws = db.ensure_default_workspace(); db.set_active_workspace(ws)
    fid = db.add_folder('/p', name='p')
    p1 = db.add_photo(folder_id=fid, filename='low.jpg', extension='.jpg',
                      file_size=1, file_mtime=1.0)
    p2 = db.add_photo(folder_id=fid, filename='high.jpg', extension='.jpg',
                      file_size=1, file_mtime=2.0)
    db.update_photo_quality_score(p1, 0.2)  # confirm method name; else UPDATE photos SET quality_score
    db.update_photo_quality_score(p2, 0.9)
    k = db.add_keyword('Song Sparrow'); db.tag_photo(p1, k); db.tag_photo(p2, k)
    db.conn.execute("UPDATE keywords SET is_species=1, taxon_id=? WHERE id=?",
                    (ids['Melospiza melodia'], k)); db.conn.commit()
    best = db.get_life_list_best_photo_by_taxon([ids['Melospiza melodia']])
    assert best[ids['Melospiza melodia']]['filename'] == 'high.jpg'
```

> NOTE: confirm quality-score setter name in db.py; fall back to raw UPDATE if absent.

**Step 2: Run — expect FAIL.**

**Step 3: Implement** (one representative photo per taxon, workspace-scoped, best quality_score then newest):

```python
def get_life_list_best_photo_by_taxon(self, taxon_ids):
    """Map taxon_id -> {id, filename} of a representative (highest quality_score,
    newest) workspace-scoped photo for that species. Missing taxa are absent."""
    ids = [t for t in taxon_ids if t is not None]
    if not ids:
        return {}
    ws = self._ws_id()
    placeholders = ",".join("?" for _ in ids)
    rows = self.conn.execute(
        f"""SELECT k.taxon_id AS tid, p.id, p.filename, p.quality_score, p.timestamp
            FROM photo_keywords pk
            JOIN keywords k ON k.id = pk.keyword_id AND k.taxon_id IN ({placeholders})
            JOIN photos p ON p.id = pk.photo_id
            JOIN workspace_folders wf ON wf.folder_id = p.folder_id
             AND wf.workspace_id = ?
            JOIN folders f ON f.id = p.folder_id AND f.status IN ('ok','partial')
            WHERE COALESCE(p.flag, 'none') != 'rejected'
            ORDER BY k.taxon_id,
                     COALESCE(p.quality_score, -1) DESC,
                     COALESCE(p.timestamp, '') DESC""",
        ids + [ws],
    ).fetchall()
    best = {}
    for r in rows:
        if r["tid"] not in best:  # first row per taxon is the best by ORDER BY
            best[r["tid"]] = {"id": r["id"], "filename": r["filename"]}
    return best
```

**Step 4: Run — expect PASS.**

**Step 5: Commit** `feat(life-list): representative photo per species taxon`.

---

### Task 6: Payload builder — roll up the completeness tree (pure function)

**Files:**
- Modify: `vireo/app.py` (add `_build_explorer_payload(db, root_id=None)` near `_build_life_list_payload`, ~line 6931)
- Test: `vireo/tests/test_app.py`

The rollup is pure Python over Task 3's subtree + Task 2's found set. Compute per node: `found_species`, `total_species`, and for immediate children `found_children`/`total_children`; plus a top-line `summary` of found/total per rank.

**Step 1: Failing test** (unit-test the builder directly with the seeded tree):

```python
def test_build_explorer_payload_rollup(db):
    from app import _build_explorer_payload
    ids = _seed_bird_taxonomy(db)
    ws = db.ensure_default_workspace(); db.set_active_workspace(ws)
    fid = db.add_folder('/p', name='p')
    p = db.add_photo(folder_id=fid, filename='a.jpg', extension='.jpg',
                     file_size=1, file_mtime=1.0)
    k = db.add_keyword('Song Sparrow'); db.tag_photo(p, k)
    db.conn.execute("UPDATE keywords SET is_species=1, taxon_id=? WHERE id=?",
                    (ids['Melospiza melodia'], k)); db.conn.commit()

    payload = _build_explorer_payload(db)
    assert payload['taxonomy_ready'] is True
    assert payload['root']['name'] == 'Aves'
    s = payload['summary']
    assert s['species'] == {'found': 1, 'total': 4}     # 4 species seeded
    assert s['genus']   == {'found': 1, 'total': 3}
    assert s['family']  == {'found': 1, 'total': 2}
    assert s['order']   == {'found': 1, 'total': 2}
    # Passeriformes order node carries family child counts + species rollup
    orders = {n['name']: n for n in payload['nodes']}
    passeri = orders['Passeriformes']
    assert passeri['found_species'] == 1 and passeri['total_species'] == 3
    assert passeri['child_rank'] == 'family'
    assert passeri['found_children'] == 1 and passeri['total_children'] == 1

def test_build_explorer_payload_not_ready(db):
    from app import _build_explorer_payload
    ws = db.ensure_default_workspace(); db.set_active_workspace(ws)
    payload = _build_explorer_payload(db)
    assert payload['taxonomy_ready'] is False
    assert payload['nodes'] == []
```

**Step 2: Run — expect FAIL.**

**Step 3: Implement** in `vireo/app.py`:

```python
_EXPLORER_RANKS = ["order", "family", "genus", "species"]
_EXPLORER_CHILD_RANK = {"class": "order", "order": "family",
                        "family": "genus", "genus": "species"}

def _build_explorer_payload(db, root_id=None):
    """Completeness tree (down to genus) for one class. Species leaves load
    separately via _build_explorer_species. Honest about the two failure
    states: taxonomy-not-downloaded and unmatched species."""
    root = (db.get_explorer_root() if root_id is None
            else db.get_taxon_by_id(root_id))   # add thin getter or inline SELECT
    if root is None:
        return {"taxonomy_ready": False, "root": None, "summary": {},
                "nodes": [], "unmatched_species": {"count": 0, "names": []},
                "classes": []}

    rows = db.get_taxon_subtree(root["id"])
    found = db.get_life_list_taxon_ids()

    # Build node index + children lists.
    nodes = {r["id"]: {**r, "children": [], "found_species": 0,
                       "total_species": 0} for r in rows}
    for n in nodes.values():
        pid = n["parent_id"]
        if pid in nodes and pid != n["id"]:
            nodes[pid]["children"].append(n)

    # Post-order rollup of species totals/found.
    def rollup(node):
        if node["rank"] == "species":
            node["total_species"] = 1
            node["found_species"] = 1 if node["id"] in found else 0
        else:
            for c in node["children"]:
                rollup(c)
                node["total_species"] += c["total_species"]
                node["found_species"] += c["found_species"]
    rollup(nodes[root["id"]])

    # Per-rank summary across the whole class.
    summary = {r: {"found": 0, "total": 0} for r in _EXPLORER_RANKS}
    for n in nodes.values():
        if n["rank"] in summary:
            summary[n["rank"]]["total"] += 1
            if n["found_species"] > 0:
                summary[n["rank"]]["found"] += 1

    def to_out(node):
        child_rank = _EXPLORER_CHILD_RANK.get(node["rank"])
        kids = [c for c in node["children"]]
        found_children = sum(1 for c in kids if c["found_species"] > 0)
        out = {
            "id": node["id"], "name": node["name"],
            "common_name": node["common_name"], "rank": node["rank"],
            "found_species": node["found_species"],
            "total_species": node["total_species"],
            "child_rank": child_rank,
            "found_children": found_children,
            "total_children": len(kids),
        }
        # Materialize tree down to genus; species leaves load on demand.
        if node["rank"] != "genus":
            out["children"] = [to_out(c) for c in
                               sorted(kids, key=lambda c: (c["found_species"] == 0,
                                      (c["common_name"] or c["name"]).lower()))]
        else:
            out["children"] = []  # species fetched via leaf endpoint
        return out

    top = [to_out(c) for c in sorted(nodes[root["id"]]["children"],
           key=lambda c: (c["found_species"] == 0,
                          (c["common_name"] or c["name"]).lower()))]

    unmatched = db.get_life_list_unmatched_species()
    classes = db.get_classes_for_taxa(found)
    return {
        "taxonomy_ready": True,
        "root": {"id": root["id"], "name": root["name"],
                 "common_name": root.get("common_name"), "rank": root["rank"]},
        "summary": summary,
        "nodes": top,
        "unmatched_species": {"count": len(unmatched), "names": unmatched[:200]},
        "classes": classes,
    }
```

> Add a tiny `db.get_taxon_by_id(id)` helper (SELECT id,name,common_name,rank,parent_id) if not present — used for non-default roots.

**Step 4: Run — expect PASS.**

**Step 5: Commit** `feat(life-list): explorer completeness rollup builder`.

---

### Task 7: Species-leaf builder (found + missing under a genus)

**Files:** Modify `vireo/app.py`; Test `vireo/tests/test_app.py`.

**Step 1: Failing test**

```python
def test_build_explorer_species_leaf(db):
    from app import _build_explorer_species
    ids = _seed_bird_taxonomy(db)
    ws = db.ensure_default_workspace(); db.set_active_workspace(ws)
    fid = db.add_folder('/p', name='p')
    p = db.add_photo(folder_id=fid, filename='a.jpg', extension='.jpg',
                     file_size=1, file_mtime=1.0)
    k = db.add_keyword('Song Sparrow'); db.tag_photo(p, k)
    db.conn.execute("UPDATE keywords SET is_species=1, taxon_id=? WHERE id=?",
                    (ids['Melospiza melodia'], k)); db.conn.commit()
    out = _build_explorer_species(db, ids['Melospiza'])
    by = {s['name']: s for s in out['species']}
    assert by['Melospiza melodia']['found'] is True
    assert by['Melospiza melodia']['photo']['filename'] == 'a.jpg'
    assert by['Melospiza georgiana']['found'] is False
    assert by['Melospiza georgiana'].get('photo') is None
    # found first, then missing; each block alphabetical
    assert [s['found'] for s in out['species']] == [True, False]
```

**Step 2: Run — expect FAIL.**

**Step 3: Implement**

```python
def _build_explorer_species(db, genus_id):
    """Found+missing species directly under a genus, found ones with a
    representative photo. Found sorted first, then missing; each alphabetical."""
    rows = [r for r in db.get_taxon_subtree(genus_id, max_depth=1)
            if r["rank"] == "species"]
    found = db.get_life_list_taxon_ids()
    found_ids = [r["id"] for r in rows if r["id"] in found]
    photos = db.get_life_list_best_photo_by_taxon(found_ids)
    species = []
    for r in rows:
        is_found = r["id"] in found
        species.append({
            "id": r["id"], "name": r["name"], "common_name": r["common_name"],
            "found": is_found,
            "photo": photos.get(r["id"]) if is_found else None,
        })
    species.sort(key=lambda s: (not s["found"],
                                (s["common_name"] or s["name"]).lower()))
    return {"genus_id": genus_id, "species": species}
```

**Step 4: Run — expect PASS.**

**Step 5: Commit** `feat(life-list): explorer species leaf builder`.

---

### Task 8: Endpoints — `/api/life-list/explorer` and species leaf

**Files:** Modify `vireo/app.py` (near `api_life_list`, ~line 6940); Test `vireo/tests/test_app.py`.

**Step 1: Failing test** (through the Flask client; extend `app_and_db` seed or seed inline). Add taxa + link a keyword inside the test:

```python
def test_api_explorer_endpoint(app_and_db):
    app, db = app_and_db
    ids = _seed_bird_taxonomy(db)
    # Link the fixture's existing 'Cardinal' keyword to a bird taxon so it counts.
    db.conn.execute("UPDATE keywords SET is_species=1, taxon_id=? WHERE name='Cardinal'",
                    (ids['Melospiza melodia'],)); db.conn.commit()
    client = app.test_client()
    r = client.get('/api/life-list/explorer')
    assert r.status_code == 200
    data = r.get_json()
    assert data['taxonomy_ready'] is True
    assert data['root']['name'] == 'Aves'
    assert data['summary']['order']['total'] == 2
    # species leaf
    r2 = client.get(f"/api/life-list/explorer/species?genus={ids['Melospiza']}")
    assert r2.status_code == 200
    assert {s['name'] for s in r2.get_json()['species']} == \
        {'Melospiza melodia', 'Melospiza georgiana'}

def test_api_explorer_not_ready(app_and_db):
    app, db = app_and_db  # fixture has no taxa
    r = app.test_client().get('/api/life-list/explorer')
    assert r.status_code == 200
    assert r.get_json()['taxonomy_ready'] is False
```

**Step 2: Run — expect FAIL.**

**Step 3: Implement** (match existing route style, `db = _get_db()`):

```python
@app.route("/api/life-list/explorer")
def api_life_list_explorer():
    db = _get_db()
    root_id = request.args.get("root", type=int)  # None -> default Aves
    return jsonify(_build_explorer_payload(db, root_id=root_id))

@app.route("/api/life-list/explorer/species")
def api_life_list_explorer_species():
    db = _get_db()
    genus_id = request.args.get("genus", type=int)
    if not genus_id:
        return jsonify({"genus_id": None, "species": []})
    return jsonify(_build_explorer_species(db, genus_id))
```

> `?root` accepts a taxa id (from the class selector). The bare word "Aves" is handled server-side by defaulting when `root` is absent, so the frontend passes ids only.

**Step 4: Run — expect PASS.** Then run the full backend suite:
`python -m pytest vireo/tests/test_db.py vireo/tests/test_app.py -q`

**Step 5: Commit** `feat(life-list): explorer API endpoints`.

---

## STAGE 2 — Frontend: tabs + drill-down + leaf + honesty states

### Task 9: Add List/Explorer tabs to life_list.html (List unchanged)

**Files:** Modify `vireo/templates/life_list.html`.

**Steps:**
1. Wrap the existing controls bar + `#content` + `#emptyState` in a panel `<div class="ll-tabpanel active" id="tab-list">`.
2. Add a tab bar above the panels (copy `.tabs/.tab/.tab-panel` CSS + `switchTab` JS from `audit.html:41-81,180-187`, renamed to `ll-tab*` to avoid collisions):
   ```html
   <div class="ll-tabs">
     <div class="ll-tab active" data-tab="list" onclick="llSwitchTab('list')">List</div>
     <div class="ll-tab" data-tab="explorer" onclick="llSwitchTab('explorer')">Explorer</div>
   </div>
   ```
3. Add empty `<div class="ll-tabpanel" id="tab-explorer"></div>` panel (filled by later tasks).
4. `llSwitchTab(name)`: toggle `.active` on `.ll-tab` and `.ll-tabpanel` (`id === 'tab-'+name`); push `?view=<name>` via `history.replaceState`; on first switch to explorer, call `loadExplorer()` (Task 10) once (guard with a `explorerLoaded` flag).
5. On page load, read `?view=` and activate that tab (default `list`).

**Verify:** `python vireo/app.py --db ~/.vireo/vireo.db --port 8080` → open `/life-list`, confirm List tab looks identical to before and the Explorer tab switches. Commit `feat(life-list): List/Explorer tab shell`.

---

### Task 10: Explorer data load + honesty states + class selector

**Files:** Modify `vireo/templates/life_list.html` (JS + CSS in the inline blocks).

**Behavior:**
- `loadExplorer(rootId)` → `safeFetch('/api/life-list/explorer' + (rootId?('?root='+rootId):''))`, store in `explorerData`, call `renderExplorer()`.
- **Not ready:** if `!explorerData.taxonomy_ready`, render a full-panel CTA into `#tab-explorer`: heading "Download the taxonomy to see completeness", body explaining it needs the reference taxonomy, and a button that POSTs `/api/jobs/download-taxonomy` (reuse existing job-trigger pattern; the app already polls jobs). Do NOT show fake 0/0.
- **Empty class:** if ready but `summary.species.total===0` for the chosen root, show "No reference taxa for this class." If `summary.species.found===0`, still render the tree (all missing) — that's valid.
- **Class selector:** a `<select id="explorerClass">` populated from `explorerData.classes` (value = class id), defaulting to the current root; `Birds`/`Aves` label uses `common_name || name`. On change → `loadExplorer(value)`. If the default root (Aves) isn't in `classes` (user has no birds yet) still include it as the first option so birds is always selectable.
- **Unmatched footnote:** if `unmatched_species.count > 0`, render a small muted line under the summary: `"<n> tagged species aren't matched to the taxonomy and aren't counted here"` with a `<details>` listing `unmatched_species.names`.

**CSS:** reuse theme vars; CTA uses `.publish-btn` style. **Verify** the three states by toggling taxonomy in a scratch DB (or temporarily forcing `taxonomy_ready:false`). Commit `feat(life-list): explorer load + honesty states + class selector`.

---

### Task 11: Summary bar + breadcrumb + card grid drill-down

**Files:** Modify `vireo/templates/life_list.html`.

**State:** `explorerPath = []` (array of `{id,name,rank}` from class root down). Current level = children at `explorerPath`'s tail (or top-level `explorerData.nodes` when path empty).

**Render:**
1. **Summary bar** (always visible for current root): four chips — `"30/44 orders · 120/250 families · 800/2300 genera · 1200/11000 species"` from `explorerData.summary`. Tabular-nums.
2. **Breadcrumb:** `Birds › Passeriformes › Passerellidae`, each crumb clickable → truncates `explorerPath` and re-renders. First crumb = root class label.
3. **Card grid:** one card per node at the current level. Card shows:
   - **Progress ring** (inline SVG, two `<circle>`, stroke-dasharray by `found_species/total_species`; accent stroke on `var(--bg-tertiary)` track). Center label = `pct%`.
   - Name (`common_name || name`), scientific name (italic, muted) when common differs.
   - Subtitle: `"<found_children>/<total_children> <child_rank>s · <found_species>/<total_species> species"`.
   - Dim the card (`opacity:.55`) when `found_species===0` (a group you've never touched) — honest "not started" signal.
4. **Click** a card:
   - genus card → fetch species leaf (Task 12).
   - other → push node to `explorerPath`, re-render from its `children`.

Reuse `.species-card` look (rounded, `--bg-secondary`, `--border-primary`, hover `--accent`). New classes `.ll-ring`, `.ll-summary`, `.ll-crumbs`. **Verify** drill Birds → order → family → genus in the running app. Commit `feat(life-list): explorer summary, breadcrumb, drill-down cards`.

---

### Task 12: Species leaf (found + missing)

**Files:** Modify `vireo/templates/life_list.html`.

**Behavior:** on genus click, `safeFetch('/api/life-list/explorer/species?genus='+id)`; render a grid under the breadcrumb:
- **Found** species: thumbnail (`photoThumbnailUrl(sp.photo)` → `/thumbnails/<id>.jpg`), name, common name, a lit "✓ seen" chip. Clicking the thumbnail opens the existing lightbox if easy (optional; else no-op).
- **Missing** species: dimmed card (no image, placeholder tile in `--bg-tertiary`), name, common name, a muted "not yet" chip.
- Header line: `"<found>/<total> species in <genus>"`.
- A toggle `"Show missing only"` (checkbox) to help gap-hunting (client-side filter).

**Verify** a genus where you have some but not all species shows found lit + missing dimmed. Commit `feat(life-list): explorer species leaf with found+missing`.

---

## STAGE 3 — Sunburst showpiece

### Task 13: Sunburst overview at the top of the Explorer tab

**Files:** Modify `vireo/templates/life_list.html`.

**Behavior:** inline SVG radial diagram built from `explorerData.nodes` (down to genus) + on-demand species rings are out of scope — sunburst goes class-center → orders → families → genera (3 rings). No external library.
- Compute each node's angular span proportional to its `total_species` (so big orders read bigger). Recurse to assign child spans within the parent's arc.
- Each arc: fill = `var(--accent)` at opacity scaled by `found_species/total_species` (0 → ghost `var(--bg-tertiary)`, 1 → full accent). Stroke `var(--bg-primary)` hairline between arcs.
- **Hover:** tooltip `"<name> — <found>/<total> species (<pct>%)"`.
- **Click** an arc: set `explorerPath` to that node's lineage and re-render the cards/breadcrumb below (drives the drill-down). Clicking center resets to top.
- Keep it a fixed square (e.g. 360×360), centered, above the summary bar. Degrade gracefully: if `nodes` empty, hide the sunburst.

Helper: build an id→node lineage map from `explorerData.nodes` during render so arc→path is O(1).

**Verify** the sunburst renders for birds, arcs shade by completeness, hover shows counts, clicking an arc drills the cards below. Commit `feat(life-list): sunburst completeness overview`.

---

## Final verification

1. Full project test command (from CLAUDE.md):
   ```bash
   python -m pytest tests/test_workspaces.py vireo/tests/test_db.py vireo/tests/test_app.py \
     vireo/tests/test_photos_api.py vireo/tests/test_edits_api.py vireo/tests/test_jobs_api.py \
     vireo/tests/test_darktable_api.py vireo/tests/test_config.py -v
   ```
   Expected: green (ignore the known pre-existing failures noted in project memory).
2. Manual: run the app, download taxonomy if needed, drill Birds → order → family → genus → species, switch the class selector to Mammalia, confirm the unmatched-species footnote appears when you have species keywords without `taxon_id`.
3. `gh pr create --base main` with a summary of what changed + test results. Stages 1/2/3 can be separate commits in one PR (each was independently shippable).

## Notes / DRY / YAGNI

- Reuse `_get_db()`, `safeFetch`, `photoThumbnailUrl`, `.species-card`, `.publish-btn`, and the `audit.html` tab pattern rather than inventing new ones.
- Do NOT ship a fake denominator when taxonomy is absent — the CTA state is a hard requirement (CORE_PHILOSOPHY "No black boxes").
- Confirm helper names before use: photo flag setter, quality-score setter, `get_taxon_by_id`. Add thin getters only if missing.
- Class selector, sunburst species rings, lightbox-on-leaf-click are the only "nice to have" edges — everything else is core.
