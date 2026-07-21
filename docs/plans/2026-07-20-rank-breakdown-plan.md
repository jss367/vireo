# Life List Explorer — clickable rank breakdown

> **For Claude:** implement task-by-task with TDD.

**Goal:** Make the Explorer's summary chips (`17/47 orders`, `56/258 families`, `143/2409 genera`, `185/11355 species`) clickable. Clicking a chip opens a **flat list of every taxon at that rank under the current class**, seen ones lit and unseen dimmed, with a search box and a "Show missing only" filter. This answers "show me all families (or genera/species) and which I haven't seen" in one screen — the drill-down cards only show one branch at a time.

**Architecture:** New server endpoint `GET /api/life-list/explorer/rank` returns the flat, server-authoritative list at a rank (so counts always match the chips). Frontend: chips become clickable, a new flat "rank view" render mode reuses existing tile/card styling + the missing-only pattern; a breadcrumb ("Birds › All families") returns to the drill view.

**Base:** `origin/main` @ 1530b0cc (explorer already merged). All anchors verified below.

---

## Background facts (verified in this worktree)

- `vireo/app.py`:
  - `_EXPLORER_RANKS = ["order","family","genus","species"]` (line 2004); `_EXPLORER_CHILD_RANK` (2005).
  - `_build_explorer_payload(db, root_id=None)` (2009) — resolves root via `db.get_explorer_root()`/`db.get_taxon_by_id(root_id)`, guards `root["rank"]!="class"` → `valid_root:False`, builds subtree via `db.get_taxon_subtree`, found set via `db.get_life_list_taxon_ids()`, post-order rollup of `found_species`/`total_species`. **Reuse this exact rollup pattern.**
  - `_build_explorer_species(db, genus_id)` (2112) — returns `{genus_id, species:[{id,name,common_name,found,photo}]}` using `db.get_life_list_best_photo_by_taxon`. Good reference for the species case.
  - Routes registered inside `create_app`: `api_life_list_explorer` (10781, `GET /api/life-list/explorer`), `api_life_list_explorer_species` (10787). Add the new route next to these. Pattern: `db = _get_db()`, `request.args.get(..., type=...)`, `return jsonify(...)`.
- DB helpers available: `get_explorer_root`, `get_taxon_by_id`, `get_taxon_subtree(root_id, max_depth=12)`, `get_life_list_taxon_ids()`, `get_life_list_best_photo_by_taxon(taxon_ids)`.
- `vireo/templates/life_list.html`:
  - `.ll-sumchip` CSS (85-86) — currently plain spans, no cursor/handler.
  - `renderSummaryBar(summary)` (966) — returns the chips; `chip(v,label)` at 972 emits `<span class="ll-sumchip">`.
  - `renderExplorer()` (865) sets panel innerHTML then wires sunburst + breadcrumb. `renderExplorerBody()` (994) renders the current drill level (cards or species leaf). `renderBreadcrumb()` (1020) + delegated `wireBreadcrumb(container)` (933). `currentExplorerNodes()` (983). `loadExplorerSpecies` (1300), `renderExplorerLeaf(missingOnly)` (1314). `explorerPath` global (1365). Species tiles + "Show missing only" pattern live in `renderExplorerLeaf`.
  - Helpers: `escapeHtml`, `escapeAttr`, `rankPlural(rank)`, `photoThumbnailUrl(photo)`, `safeFetch`, `renderRing(found,total)`.
- Test fixtures: `vireo/tests/conftest.py` `db` + `app_and_db`. `_seed_bird_taxonomy(db)` helper exists in `vireo/tests/test_db.py` and `test_app.py` (bird subtree: Aves→Passeriformes/Anseriformes→families→genera→species). Ruff forbids `;` multi-statements in tests (E702) — one statement per line.

---

## STAGE 1 — Backend: flat rank endpoint

### Task 1: `_build_explorer_rank(db, rank, root_id=None)` builder

**Files:** Modify `vireo/app.py` (add after `_build_explorer_species`, ~line 2150). Test: `vireo/tests/test_app.py`.

**Behavior:** Return every taxon at `rank` under the class root, flagged found/unfound, sorted found-first then alpha. A non-species taxon is "found" iff its subtree contains ≥1 found species (reuse the `found_species>0` rollup). Species are found iff in the found set, and found species carry a representative photo.

Return shape:
```python
{
  "taxonomy_ready": bool,
  "valid_root": bool,
  "rank": rank,                     # echoed back
  "root": {"id","name","common_name","rank"} | None,
  "found": int, "total": int,       # must equal summary[rank] from _build_explorer_payload
  "items": [
     {"id","name","common_name","rank","found": bool,
      "found_species": int, "total_species": int,   # for the ring/counts on non-species rows
      "order": <ancestor order common||name or None>,  # context label
      "photo": {"id","filename"} | None }            # only for found species
  ]
}
```

**Step 1 — failing test** (`test_app.py`): seed bird taxonomy, tag one species (link a keyword `taxon_id` to `ids['Melospiza melodia']` on a workspace-visible non-rejected photo — copy the setup already used by the explorer payload tests in this file). Then:
```python
def test_build_explorer_rank_families(db):
    from app import _build_explorer_rank
    ids = _seed_bird_taxonomy(db)
    ws = db.ensure_default_workspace()
    db.set_active_workspace(ws)
    fid = db.add_folder('/p', name='p')
    p = db.add_photo(folder_id=fid, filename='a.jpg', extension='.jpg',
                     file_size=1, file_mtime=1.0)
    k = db.add_keyword('Song Sparrow')
    db.tag_photo(p, k)
    db.conn.execute("UPDATE keywords SET is_species=1, taxon_id=? WHERE id=?",
                    (ids['Melospiza melodia'], k))
    db.conn.commit()
    out = _build_explorer_rank(db, 'family')
    assert out['rank'] == 'family'
    assert out['total'] == 2          # Passerellidae, Anatidae (+Cardinalidae/Corvidae if seeded) -> match seed
    by = {i['name']: i for i in out['items']}
    assert by['Passerellidae']['found'] is True
    assert by['Anatidae']['found'] is False
    # found-first ordering
    assert out['items'][0]['found'] is True
    # counts equal the payload summary
    from app import _build_explorer_payload
    assert out['found'] == _build_explorer_payload(db)['summary']['family']['found']
    assert out['total'] == _build_explorer_payload(db)['summary']['family']['total']

def test_build_explorer_rank_species_has_photo(db):
    from app import _build_explorer_rank
    ids = _seed_bird_taxonomy(db)
    # ... same tagging setup ...
    out = _build_explorer_rank(db, 'species')
    by = {i['name']: i for i in out['items']}
    assert by['Melospiza melodia']['found'] is True
    assert by['Melospiza melodia']['photo']['filename'] == 'a.jpg'
    assert by['Melospiza georgiana']['found'] is False
    assert by['Melospiza georgiana']['photo'] is None
```
> Match assertion numbers to whatever `_seed_bird_taxonomy` actually contains in THIS file — read the helper first and set expected totals accordingly. Keep numbers derived from the seed, not guessed.

**Step 2:** run — expect FAIL (`_build_explorer_rank` missing).

**Step 3 — implement.** Reuse the payload's subtree + rollup; do NOT duplicate scope logic:
```python
def _build_explorer_rank(db, rank, root_id=None):
    """Flat list of all taxa at `rank` under the class root, each flagged
    found/unfound. Server-authoritative so it agrees with the summary chips."""
    if rank not in _EXPLORER_RANKS:
        rank = "family"
    root = (db.get_explorer_root() if root_id is None else db.get_taxon_by_id(root_id))
    empty = {"taxonomy_ready": root is not None, "valid_root": False,
             "rank": rank, "root": None, "found": 0, "total": 0, "items": []}
    if root is None:
        empty["taxonomy_ready"] = False
        return empty
    if root["rank"] != "class":
        return empty

    rows = db.get_taxon_subtree(root["id"])
    found = db.get_life_list_taxon_ids()
    nodes = {r["id"]: {**r, "children": [], "found_species": 0, "total_species": 0}
             for r in rows}
    for n in nodes.values():
        pid = n["parent_id"]
        if pid in nodes and pid != n["id"]:
            nodes[pid]["children"].append(n)

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

    def order_label(node):
        cur = node["parent_id"]
        while cur in nodes and nodes[cur]["rank"] != "order":
            cur = nodes[cur]["parent_id"]
        if cur in nodes and nodes[cur]["rank"] == "order":
            o = nodes[cur]
            return o["common_name"] or o["name"]
        return None

    targets = [n for n in nodes.values() if n["rank"] == rank]
    found_species_ids = [n["id"] for n in targets
                         if rank == "species" and n["found_species"] > 0]
    photos = db.get_life_list_best_photo_by_taxon(found_species_ids) if found_species_ids else {}

    items = []
    for n in targets:
        is_found = n["found_species"] > 0
        items.append({
            "id": n["id"], "name": n["name"], "common_name": n["common_name"],
            "rank": rank, "found": is_found,
            "found_species": n["found_species"], "total_species": n["total_species"],
            "order": order_label(n),
            "photo": photos.get(n["id"]) if (rank == "species" and is_found) else None,
        })
    items.sort(key=lambda i: (not i["found"], (i["common_name"] or i["name"]).lower()))
    return {
        "taxonomy_ready": True, "valid_root": True, "rank": rank,
        "root": {"id": root["id"], "name": root["name"],
                 "common_name": root.get("common_name"), "rank": root["rank"]},
        "found": sum(1 for i in items if i["found"]), "total": len(items),
        "items": items,
    }
```

**Step 4:** run — expect PASS. **Step 5:** commit `feat(life-list): flat rank breakdown builder`.

### Task 2: route `GET /api/life-list/explorer/rank`

**Files:** `vireo/app.py` (next to `api_life_list_explorer_species`, ~10787). Test: `test_app.py`.

**Test (through client):**
```python
def test_api_explorer_rank(app_and_db):
    app, db = app_and_db
    ids = _seed_bird_taxonomy(db)
    db.conn.execute("UPDATE keywords SET is_species=1, taxon_id=? WHERE name='Cardinal'",
                    (ids['Melospiza melodia'],))
    db.conn.commit()
    r = app.test_client().get('/api/life-list/explorer/rank?rank=family')
    assert r.status_code == 200
    d = r.get_json()
    assert d['rank'] == 'family'
    assert d['total'] >= 2
    assert any(i['found'] for i in d['items'])

def test_api_explorer_rank_defaults_bad_rank(app_and_db):
    app, db = app_and_db
    r = app.test_client().get('/api/life-list/explorer/rank?rank=bogus')
    assert r.status_code == 200   # falls back to a valid rank, not 500
```

**Implement:**
```python
@app.route("/api/life-list/explorer/rank")
def api_life_list_explorer_rank():
    db = _get_db()
    rank = request.args.get("rank", "family")
    root_id = request.args.get("root", type=int)
    return jsonify(_build_explorer_rank(db, rank, root_id=root_id))
```
Commit `feat(life-list): flat rank breakdown endpoint`.

Run backend suite: `python -m pytest vireo/tests/test_db.py vireo/tests/test_app.py -q`.

---

## STAGE 2 — Frontend: clickable chips + flat rank view

All in `vireo/templates/life_list.html`. No TDD harness for JS; implement carefully, `node --check` the extracted script, keep tests green, maintainer browser-verifies.

### Task 3: make chips clickable

- `.ll-sumchip` CSS: add `cursor:pointer;` and a hover (`border-color:var(--accent);`). Add an `.active` style (accent border/text) for the currently-open rank.
- In `renderSummaryBar`, give each chip `data-rank="order|family|genus|species"` (add a `rank` arg to the `chip()` helper — chips are `order/family/genus/species` in that fixed order). Mark `.active` when it matches the open rank view.
- Wire clicks via **delegation** on the persistent explorer container (mirror `wireBreadcrumb`): one listener that reads `data-rank` off `closest('.ll-sumchip')` and calls `openRankView(rank)`.

### Task 4: flat rank view render + state

- New globals: `explorerRankView = null` (holds `{rank, data, search, missingOnly}` when active).
- `openRankView(rank)`: `safeFetch('/api/life-list/explorer/rank?rank='+rank + (currentRootId?('&root='+currentRootId):''))` (use the same root the explorer loaded with — reuse whatever variable holds the current class id; if none, omit `root`). Store data, set `explorerRankView`, re-render.
- Hook into `renderExplorer()` / `renderExplorerBody()`: when `explorerRankView` is set, render the **flat panel** instead of the drill cards:
  - Breadcrumb: `Birds › All <rankPlural(rank)>` — clicking `Birds` (root) or a "← Back" clears `explorerRankView` and returns to the normal card view (`explorerPath` unchanged/empty).
  - Header: `"<found>/<total> <rankPlural(rank)> seen"`.
  - Controls: a search `<input>` (filters by common/scientific name, debounced like the List tab) + a `Show missing only` checkbox.
  - Grid:
    - order/family/genus items → reuse the `.ll-card` look: `renderRing(found_species,total_species)` + name + scientific + `found_species/total_species species` + `order` context label; `.empty` when `!found`. Clicking a row **drills into it** (set `explorerPath` to its lineage and clear `explorerRankView`) — build lineage client-side from `explorerData.nodes`, or (simplest) for order/family just set path; for genus, open its species leaf. If lineage building is non-trivial, it is acceptable for v1 to make rows non-navigable and rely on the existing cards/sunburst for drilling — but PREFER click-through for order/family/genus.
    - species items → reuse the species-tile markup from `renderExplorerLeaf` (thumbnail for found via `photoThumbnailUrl`, "✓ seen" / "not yet" chips, dimmed when missing).
  - Escape every interpolated name/filename.
- Reset `explorerRankView = null` on class change (in the same place `explorerPath` is reset, ~line 843) so switching class exits the flat view.

### Task 5: performance guard for large ranks

- Genera (~2409) and species (~11355) produce large DOMs. Render lightweight rows and:
  - Default is fine, but when `items.length > 800` and neither a search term nor "missing only" is active, show a small notice above the grid: `"Showing all <n>. Use search or 'Show missing only' to narrow."` and cap the initial render to the first N (e.g. 500) with a "Show all <n>" button — OR render all if performance is acceptable in your testing. Do NOT silently truncate without a visible notice (No-black-boxes).

Verify: `python -m pytest vireo/tests/test_app.py -q` green; `node --check` on extracted JS; Jinja parse. Commit per task (`feat(life-list): clickable summary chips`, `feat(life-list): flat rank breakdown view`, `feat(life-list): large-rank render guard`).

---

## Final verification
- Full suite: `python -m pytest tests/test_workspaces.py vireo/tests/test_db.py vireo/tests/test_app.py vireo/tests/test_photos_api.py vireo/tests/test_edits_api.py vireo/tests/test_jobs_api.py vireo/tests/test_darktable_api.py vireo/tests/test_config.py -q`.
- Maintainer browser-verifies: click each chip → flat list, seen lit / unseen dimmed, search + missing-only work, row click drills, breadcrumb returns, class switch exits the view.
- PR against main.
