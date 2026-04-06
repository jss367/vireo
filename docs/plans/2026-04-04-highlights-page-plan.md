# Highlights Page Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Add a dedicated Highlights page that surfaces the best, most diverse photos from a pipeline run with interactive tuning controls and save-to-collection.

**Architecture:** New `GET /api/highlights` endpoint queries photos with quality scores and DINO embeddings, runs MMR selection server-side, returns ranked results. A new `highlights.html` template renders a top controls bar (folder picker, sliders) and photo grid. Save creates a static collection via existing `add_collection()`. All queries are workspace-scoped.

**Tech Stack:** Python/Flask backend, SQLite queries, numpy for MMR/cosine similarity, vanilla JS frontend with inline CSS.

**Design doc:** `docs/plans/2026-04-04-highlights-page-design.md`

---

### Task 1: Database method — get_highlights_candidates

**Files:**
- Modify: `vireo/db.py` (after `get_species_keywords_for_photos` at line ~2082)
- Test: `vireo/tests/test_db.py`

**Step 1: Write the failing test**

Add to `vireo/tests/test_db.py`:

```python
def test_get_highlights_candidates(tmp_path):
    """get_highlights_candidates returns photos with quality scores, species, and embeddings."""
    db = make_test_db(tmp_path)
    # Insert a folder and link to workspace
    fid = db.conn.execute(
        "INSERT INTO folders (path, name, status) VALUES (?, ?, 'ok')",
        ("/test/folder", "folder"),
    ).lastrowid
    db.conn.execute(
        "INSERT INTO workspace_folders (workspace_id, folder_id) VALUES (?, ?)",
        (db._ws_id(), fid),
    )
    # Insert photos with varying quality scores
    for i, qs in enumerate([0.9, 0.7, 0.5, 0.3, None]):
        pid = db.conn.execute(
            "INSERT INTO photos (folder_id, filename, quality_score, flag) VALUES (?, ?, ?, 'none')",
            (fid, f"img{i}.jpg", qs),
        ).lastrowid
        if qs is not None and qs >= 0.5:
            # Add a detection + accepted prediction for photos with decent quality
            did = db.conn.execute(
                "INSERT INTO detections (photo_id, workspace_id, detector_confidence) VALUES (?, ?, 0.9)",
                (pid, db._ws_id()),
            ).lastrowid
            db.conn.execute(
                "INSERT INTO predictions (detection_id, species, confidence, status) VALUES (?, ?, 0.95, 'accepted')",
                (did, f"Species{i}"),
            )
    db.conn.commit()

    # min_quality=0.5 should return 3 photos (0.9, 0.7, 0.5), excluding None and 0.3
    results = db.get_highlights_candidates(folder_id=fid, min_quality=0.5)
    assert len(results) == 3
    # Should be ordered by quality_score DESC
    scores = [r["quality_score"] for r in results]
    assert scores == sorted(scores, reverse=True)
    # Each result should have species field (may be None for unclassified)
    assert all("species" in dict(r) for r in results)


def test_get_highlights_candidates_excludes_rejected(tmp_path):
    """Flagged-rejected photos are excluded."""
    db = make_test_db(tmp_path)
    fid = db.conn.execute(
        "INSERT INTO folders (path, name, status) VALUES (?, ?, 'ok')",
        ("/test/folder", "folder"),
    ).lastrowid
    db.conn.execute(
        "INSERT INTO workspace_folders (workspace_id, folder_id) VALUES (?, ?)",
        (db._ws_id(), fid),
    )
    db.conn.execute(
        "INSERT INTO photos (folder_id, filename, quality_score, flag) VALUES (?, 'good.jpg', 0.8, 'none')",
        (fid,),
    )
    db.conn.execute(
        "INSERT INTO photos (folder_id, filename, quality_score, flag) VALUES (?, 'bad.jpg', 0.9, 'rejected')",
        (fid,),
    )
    db.conn.commit()
    results = db.get_highlights_candidates(folder_id=fid, min_quality=0.0)
    assert len(results) == 1
    assert results[0]["filename"] == "good.jpg"
```

**Step 2: Run test to verify it fails**

Run: `cd /Users/julius/git/vireo/.worktrees/highlights && python -m pytest vireo/tests/test_db.py::test_get_highlights_candidates vireo/tests/test_db.py::test_get_highlights_candidates_excludes_rejected -v`
Expected: FAIL — `get_highlights_candidates` doesn't exist yet.

**Step 3: Write the implementation**

Add to `vireo/db.py` after `get_species_keywords_for_photos` (~line 2082):

```python
def get_highlights_candidates(self, folder_id, min_quality=0.0):
    """Return photos eligible for highlights selection.

    Returns photos in the given folder that have a quality_score >= min_quality
    and are not user-rejected. Includes the top accepted prediction species
    (or NULL) and DINO embeddings for MMR diversity.

    Ordered by quality_score DESC.
    """
    rows = self.conn.execute(
        """SELECT p.id, p.folder_id, p.filename, p.extension,
                  p.timestamp, p.width, p.height, p.rating, p.flag,
                  p.thumb_path, p.quality_score, p.subject_sharpness,
                  p.subject_size, p.sharpness, p.phash_crop,
                  p.dino_subject_embedding, p.dino_global_embedding,
                  bp.species
           FROM photos p
           JOIN workspace_folders wf ON wf.folder_id = p.folder_id
           LEFT JOIN (
               SELECT det.photo_id, pred.species,
                      ROW_NUMBER() OVER (
                          PARTITION BY det.photo_id
                          ORDER BY pred.confidence DESC
                      ) AS rn
               FROM detections det
               JOIN predictions pred ON pred.detection_id = det.id
               WHERE det.workspace_id = ? AND pred.status = 'accepted'
           ) bp ON bp.photo_id = p.id AND bp.rn = 1
           WHERE p.folder_id = ?
             AND wf.workspace_id = ?
             AND p.quality_score IS NOT NULL
             AND p.quality_score >= ?
             AND p.flag != 'rejected'
           ORDER BY p.quality_score DESC""",
        (self._ws_id(), folder_id, self._ws_id(), min_quality),
    ).fetchall()
    return rows
```

**Step 4: Run test to verify it passes**

Run: `cd /Users/julius/git/vireo/.worktrees/highlights && python -m pytest vireo/tests/test_db.py::test_get_highlights_candidates vireo/tests/test_db.py::test_get_highlights_candidates_excludes_rejected -v`
Expected: PASS

**Step 5: Commit**

```bash
git add vireo/db.py vireo/tests/test_db.py
git commit -m "feat(highlights): add get_highlights_candidates db method"
```

---

### Task 2: Database method — get_folders_with_quality_data

**Files:**
- Modify: `vireo/db.py` (after `get_highlights_candidates`)
- Test: `vireo/tests/test_db.py`

**Step 1: Write the failing test**

```python
def test_get_folders_with_quality_data(tmp_path):
    """Returns only folders that have photos with quality scores."""
    db = make_test_db(tmp_path)
    # Folder with quality data
    fid1 = db.conn.execute(
        "INSERT INTO folders (path, name, status) VALUES ('/scored', 'scored', 'ok')",
    ).lastrowid
    db.conn.execute(
        "INSERT INTO workspace_folders (workspace_id, folder_id) VALUES (?, ?)",
        (db._ws_id(), fid1),
    )
    db.conn.execute(
        "INSERT INTO photos (folder_id, filename, quality_score) VALUES (?, 'a.jpg', 0.8)",
        (fid1,),
    )
    # Folder without quality data
    fid2 = db.conn.execute(
        "INSERT INTO folders (path, name, status) VALUES ('/noscores', 'noscores', 'ok')",
    ).lastrowid
    db.conn.execute(
        "INSERT INTO workspace_folders (workspace_id, folder_id) VALUES (?, ?)",
        (db._ws_id(), fid2),
    )
    db.conn.execute(
        "INSERT INTO photos (folder_id, filename) VALUES (?, 'b.jpg')",
        (fid2,),
    )
    db.conn.commit()

    folders = db.get_folders_with_quality_data()
    assert len(folders) == 1
    assert folders[0]["name"] == "scored"
    assert folders[0]["photo_count"] > 0
```

**Step 2: Run test to verify it fails**

Run: `cd /Users/julius/git/vireo/.worktrees/highlights && python -m pytest vireo/tests/test_db.py::test_get_folders_with_quality_data -v`
Expected: FAIL

**Step 3: Write the implementation**

```python
def get_folders_with_quality_data(self):
    """Return folders that have at least one photo with a quality_score.

    Used to populate the folder dropdown on the highlights page.
    Returns id, path, name, and count of scored photos, ordered by most recent photo first.
    """
    return self.conn.execute(
        """SELECT f.id, f.path, f.name,
                  COUNT(p.id) as photo_count,
                  MAX(p.timestamp) as latest_photo
           FROM folders f
           JOIN workspace_folders wf ON wf.folder_id = f.id
           JOIN photos p ON p.folder_id = f.id
           WHERE wf.workspace_id = ?
             AND f.status = 'ok'
             AND p.quality_score IS NOT NULL
           GROUP BY f.id
           ORDER BY latest_photo DESC""",
        (self._ws_id(),),
    ).fetchall()
```

**Step 4: Run test to verify it passes**

Run: `cd /Users/julius/git/vireo/.worktrees/highlights && python -m pytest vireo/tests/test_db.py::test_get_folders_with_quality_data -v`
Expected: PASS

**Step 5: Commit**

```bash
git add vireo/db.py vireo/tests/test_db.py
git commit -m "feat(highlights): add get_folders_with_quality_data db method"
```

---

### Task 3: Highlights selection logic

**Files:**
- Create: `vireo/highlights.py`
- Test: `vireo/tests/test_highlights.py`

**Step 1: Write the failing test**

Create `vireo/tests/test_highlights.py`:

```python
"""Tests for highlights selection logic."""
import numpy as np
import pytest

from vireo.highlights import select_highlights


def _make_photo(pid, quality, species=None, embedding=None):
    """Helper to build a photo-like dict for testing."""
    if embedding is None:
        rng = np.random.RandomState(pid)
        embedding = rng.randn(384).astype(np.float32).tobytes()
    return {
        "id": pid,
        "quality_score": quality,
        "species": species,
        "dino_subject_embedding": embedding,
        "phash_crop": f"{pid:016x}",
    }


def test_select_highlights_basic():
    """Selects top N photos by quality with diversity."""
    photos = [_make_photo(i, 0.9 - i * 0.1) for i in range(10)]
    result = select_highlights(photos, count=3, max_per_species=5)
    assert len(result) == 3
    # First pick should be highest quality
    assert result[0]["id"] == 0


def test_select_highlights_respects_max_per_species():
    """Per-species cap is enforced."""
    photos = [_make_photo(i, 0.9 - i * 0.05, species="Eagle") for i in range(10)]
    result = select_highlights(photos, count=10, max_per_species=2)
    eagle_count = sum(1 for p in result if p["species"] == "Eagle")
    assert eagle_count <= 2


def test_select_highlights_unidentified_capped():
    """Photos without species are grouped under 'Unidentified' and capped."""
    photos = [_make_photo(i, 0.9 - i * 0.05, species=None) for i in range(10)]
    result = select_highlights(photos, count=10, max_per_species=3)
    assert len(result) <= 3


def test_select_highlights_fewer_than_count():
    """Returns all photos when fewer than count are available."""
    photos = [_make_photo(i, 0.8) for i in range(3)]
    result = select_highlights(photos, count=10, max_per_species=5)
    assert len(result) == 3


def test_select_highlights_empty():
    """Empty input returns empty output."""
    result = select_highlights([], count=10, max_per_species=5)
    assert result == []


def test_select_highlights_species_diversity():
    """With multiple species, selection includes variety."""
    photos = []
    for i, sp in enumerate(["Eagle", "Hawk", "Owl", "Finch", "Wren"]):
        for j in range(5):
            photos.append(_make_photo(i * 5 + j, 0.9 - j * 0.05, species=sp))
    result = select_highlights(photos, count=10, max_per_species=3)
    species_in_result = set(p["species"] for p in result)
    # Should have multiple species represented
    assert len(species_in_result) >= 3
```

**Step 2: Run test to verify it fails**

Run: `cd /Users/julius/git/vireo/.worktrees/highlights && python -m pytest vireo/tests/test_highlights.py -v`
Expected: FAIL — `vireo.highlights` module doesn't exist.

**Step 3: Write the implementation**

Create `vireo/highlights.py`:

```python
"""Highlights selection — picks the best, most diverse photos from a folder.

Reuses MMR selection logic from vireo.selection for quality+diversity ranking,
with an added per-species cap to ensure variety across species.
"""

import numpy as np
from collections import defaultdict

from vireo.selection import diversity_distance


def select_highlights(candidates, count, max_per_species):
    """Select highlight photos using MMR with per-species caps.

    Args:
        candidates: list of photo dicts with 'quality_score', 'species',
                    'dino_subject_embedding', 'phash_crop'
        count: target number of highlights
        max_per_species: maximum photos per species (None grouped as 'Unidentified')

    Returns:
        list of selected photo dicts, ordered by selection order (best first)
    """
    if not candidates or count <= 0:
        return []

    # Deserialize DINO embeddings from bytes to numpy arrays for cosine sim
    for p in candidates:
        emb = p.get("dino_subject_embedding")
        if isinstance(emb, (bytes, memoryview)):
            p["dino_subject_embedding"] = np.frombuffer(emb, dtype=np.float32).copy()

    # Track species counts
    species_counts = defaultdict(int)
    lam = 0.70  # quality-diversity trade-off (same as encounter-level MMR)

    selected = []
    remaining = sorted(candidates, key=lambda p: p.get("quality_score", 0), reverse=True)

    while len(selected) < count and remaining:
        best_score = -1
        best_idx = -1

        for idx, cand in enumerate(remaining):
            sp = cand.get("species") or "Unidentified"
            if species_counts[sp] >= max_per_species:
                continue

            q = cand.get("quality_score", 0)
            if not selected:
                mmr_score = q
            else:
                min_div = min(
                    diversity_distance(cand, sel) for sel in selected
                )
                mmr_score = lam * q + (1 - lam) * min_div

            if mmr_score > best_score:
                best_score = mmr_score
                best_idx = idx

        if best_idx < 0:
            break  # All remaining photos are species-capped

        pick = remaining.pop(best_idx)
        sp = pick.get("species") or "Unidentified"
        species_counts[sp] += 1
        selected.append(pick)

    return selected
```

**Step 4: Run test to verify it passes**

Run: `cd /Users/julius/git/vireo/.worktrees/highlights && python -m pytest vireo/tests/test_highlights.py -v`
Expected: PASS

**Step 5: Commit**

```bash
git add vireo/highlights.py vireo/tests/test_highlights.py
git commit -m "feat(highlights): add select_highlights with MMR + species cap"
```

---

### Task 4: API routes — GET /api/highlights and POST /api/highlights/save

**Files:**
- Modify: `vireo/app.py` (add routes after the collections section)
- Test: `vireo/tests/test_app.py`

**Step 1: Write the failing test**

Add to `vireo/tests/test_app.py`:

```python
def test_highlights_get_empty(client):
    """GET /api/highlights returns empty when no quality data exists."""
    resp = client.get("/api/highlights")
    assert resp.status_code == 200
    data = resp.get_json()
    assert data["photos"] == []
    assert "folders" in data
    assert "meta" in data


def test_highlights_get_with_data(client):
    """GET /api/highlights returns highlight photos for a folder with quality data."""
    db = _get_test_db(client)
    fid = db.conn.execute(
        "INSERT INTO folders (path, name, status) VALUES ('/highlights_test', 'highlights_test', 'ok')"
    ).lastrowid
    db.conn.execute(
        "INSERT INTO workspace_folders (workspace_id, folder_id) VALUES (?, ?)",
        (db._ws_id(), fid),
    )
    for i in range(20):
        db.conn.execute(
            "INSERT INTO photos (folder_id, filename, quality_score, flag) VALUES (?, ?, ?, 'none')",
            (fid, f"img{i}.jpg", 0.9 - i * 0.03),
        )
    db.conn.commit()

    resp = client.get(f"/api/highlights?folder_id={fid}&count=5")
    assert resp.status_code == 200
    data = resp.get_json()
    assert len(data["photos"]) == 5
    assert data["meta"]["total_in_folder"] == 20


def test_highlights_save(client):
    """POST /api/highlights/save creates a static collection."""
    db = _get_test_db(client)
    fid = db.conn.execute(
        "INSERT INTO folders (path, name, status) VALUES ('/save_test', 'save_test', 'ok')"
    ).lastrowid
    db.conn.execute(
        "INSERT INTO workspace_folders (workspace_id, folder_id) VALUES (?, ?)",
        (db._ws_id(), fid),
    )
    pid = db.conn.execute(
        "INSERT INTO photos (folder_id, filename, quality_score) VALUES (?, 'a.jpg', 0.8)",
        (fid,),
    ).lastrowid
    db.conn.commit()

    resp = client.post("/api/highlights/save", json={
        "photo_ids": [pid],
        "name": "Highlights - save_test",
    })
    assert resp.status_code == 200
    data = resp.get_json()
    assert data["ok"] is True
    assert "id" in data

    # Verify collection was created
    collections = db.get_collections()
    names = [c["name"] for c in collections]
    assert "Highlights - save_test" in names
```

**Step 2: Run test to verify it fails**

Run: `cd /Users/julius/git/vireo/.worktrees/highlights && python -m pytest vireo/tests/test_app.py::test_highlights_get_empty vireo/tests/test_app.py::test_highlights_get_with_data vireo/tests/test_app.py::test_highlights_save -v`
Expected: FAIL — routes don't exist.

**Step 3: Write the implementation**

Add to `vireo/app.py`. Find the page routes section (around line 329-378) and add the highlights page route:

```python
@app.route("/highlights")
def highlights_page():
    return render_template("highlights.html")
```

Then add the API routes (after the collections routes section):

```python
# -- Highlights --

@app.route("/api/highlights")
def api_highlights():
    db = _get_db()
    import json
    from vireo.highlights import select_highlights

    folders = db.get_folders_with_quality_data()
    if not folders:
        return jsonify({
            "photos": [],
            "meta": {"total_in_folder": 0, "eligible": 0, "species_breakdown": {}},
            "folders": [],
        })

    folder_id = request.args.get("folder_id", type=int)
    if folder_id is None:
        folder_id = folders[0]["id"]  # Most recent

    count = request.args.get("count", type=int)
    max_per_species = request.args.get("max_per_species", 5, type=int)
    min_quality = request.args.get("min_quality", 0.0, type=float)

    candidates = db.get_highlights_candidates(folder_id, min_quality=min_quality)
    total_in_folder = db.count_filtered_photos(folder_id=folder_id)

    # Adaptive default: 5% clamped to [10, 50]
    if count is None:
        count = max(10, min(50, int(len(candidates) * 0.05))) if candidates else 0

    selected = select_highlights(
        [dict(r) for r in candidates],
        count=count,
        max_per_species=max_per_species,
    )

    # Build species breakdown
    species_counts = {}
    for p in selected:
        sp = p.get("species") or "Unidentified"
        species_counts[sp] = species_counts.get(sp, 0) + 1

    # Strip binary fields before JSON response
    photo_list = []
    for p in selected:
        out = {k: v for k, v in p.items()
               if k not in ("dino_subject_embedding", "dino_global_embedding")}
        photo_list.append(out)

    return jsonify({
        "photos": photo_list,
        "meta": {
            "total_in_folder": total_in_folder,
            "eligible": len(candidates),
            "species_breakdown": species_counts,
            "avg_quality": round(sum(p.get("quality_score", 0) for p in selected) / max(len(selected), 1), 2),
        },
        "folders": [{"id": f["id"], "name": f["name"], "photo_count": f["photo_count"]} for f in folders],
    })

@app.route("/api/highlights/save", methods=["POST"])
def api_highlights_save():
    db = _get_db()
    import json

    body = request.get_json(silent=True) or {}
    photo_ids = body.get("photo_ids", [])
    name = body.get("name", "").strip()

    if not photo_ids:
        return json_error("photo_ids required")
    if not name:
        return json_error("name required")

    rules = json.dumps([{"field": "photo_ids", "value": photo_ids}])
    cid = db.add_collection(name, rules)
    return jsonify({"ok": True, "id": cid})
```

**Step 4: Run test to verify it passes**

Run: `cd /Users/julius/git/vireo/.worktrees/highlights && python -m pytest vireo/tests/test_app.py::test_highlights_get_empty vireo/tests/test_app.py::test_highlights_get_with_data vireo/tests/test_app.py::test_highlights_save -v`
Expected: PASS

**Step 5: Commit**

```bash
git add vireo/app.py vireo/tests/test_app.py
git commit -m "feat(highlights): add GET /api/highlights and POST /api/highlights/save routes"
```

---

### Task 5: Highlights page template

**Files:**
- Create: `vireo/templates/highlights.html`

**Step 1: Create the template**

Create `vireo/templates/highlights.html` following the same pattern as other pages (e.g., `browse.html`). The page has:

- Standard HTML head with `vireo-base.css` link and inline `<style>` block
- `{% include '_navbar.html' %}` for shared navbar
- Top controls bar with folder dropdown, three sliders (count, max per species, min quality), and Save button
- Photo grid container
- Summary stats line
- Inline `<script>` block with:
  - `loadHighlights()` — fetches `GET /api/highlights` with current control values, renders grid
  - Slider change handlers with 300ms debounce calling `loadHighlights()`
  - `saveAsCollection()` — prompts for name, checks for duplicates, calls `POST /api/highlights/save`
  - Photo card click → opens existing lightbox (from `_navbar.html`)
  - Grid rendering reusing the same card layout as browse (thumbnail, species label, quality badge)

Key HTML structure:

```html
<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width, initial-scale=1.0">
  <link rel="icon" type="image/png" href="/favicon.ico">
  <link rel="stylesheet" href="/static/vireo-base.css">
  <title>Vireo - Highlights</title>
  <style>
    .highlights-controls { display:flex; align-items:center; gap:20px; padding:12px 24px; background:var(--bg-secondary); border-bottom:1px solid var(--border); flex-wrap:wrap; }
    .control-group { display:flex; align-items:center; gap:8px; }
    .control-group label { font-size:13px; color:var(--text-muted); white-space:nowrap; }
    .control-group input[type=range] { width:120px; }
    .control-group select { font-size:13px; padding:4px 8px; }
    .control-value { font-size:13px; color:var(--text-primary); min-width:30px; }
    .save-btn { margin-left:auto; padding:6px 16px; background:var(--accent); color:#fff; border:none; border-radius:4px; cursor:pointer; font-size:13px; }
    .save-btn:hover { opacity:0.9; }
    .highlights-grid { display:grid; grid-template-columns:repeat(auto-fill, minmax(200px, 1fr)); gap:12px; padding:24px; }
    .highlights-card { position:relative; border-radius:6px; overflow:hidden; background:var(--bg-secondary); cursor:pointer; }
    .highlights-card img { width:100%; aspect-ratio:3/2; object-fit:cover; display:block; }
    .card-info { padding:6px 8px; font-size:12px; }
    .card-species { color:var(--text-primary); }
    .card-quality { color:var(--text-muted); float:right; }
    .highlights-summary { padding:8px 24px; font-size:13px; color:var(--text-muted); border-top:1px solid var(--border); }
    .highlights-empty { text-align:center; padding:80px 24px; color:var(--text-muted); }
    .highlights-empty a { color:var(--accent); }
    /* Save modal */
    .save-modal-overlay { display:none; position:fixed; inset:0; background:rgba(0,0,0,0.5); z-index:500; align-items:center; justify-content:center; }
    .save-modal-overlay.open { display:flex; }
    .save-modal { background:var(--bg-primary); border-radius:8px; padding:24px; min-width:360px; }
    .save-modal h3 { margin:0 0 16px; font-size:16px; }
    .save-modal input[type=text] { width:100%; padding:8px; font-size:14px; border:1px solid var(--border); border-radius:4px; box-sizing:border-box; margin-bottom:12px; background:var(--bg-secondary); color:var(--text-primary); }
    .save-modal-actions { display:flex; gap:8px; justify-content:flex-end; }
    .save-modal-actions button { padding:6px 16px; border-radius:4px; border:none; cursor:pointer; font-size:13px; }
  </style>
</head>
<body>
{% include '_navbar.html' %}

<div class="highlights-controls" id="controlsBar">
  <div class="control-group">
    <label for="folderSelect">Folder</label>
    <select id="folderSelect"></select>
  </div>
  <div class="control-group">
    <label for="countSlider">Count</label>
    <input type="range" id="countSlider" min="1" max="100" value="20">
    <span class="control-value" id="countValue">20</span>
  </div>
  <div class="control-group">
    <label for="speciesSlider">Max/species</label>
    <input type="range" id="speciesSlider" min="1" max="20" value="5">
    <span class="control-value" id="speciesValue">5</span>
  </div>
  <div class="control-group">
    <label for="qualitySlider">Min quality</label>
    <input type="range" id="qualitySlider" min="0" max="100" value="0" step="5">
    <span class="control-value" id="qualityValue">0.00</span>
  </div>
  <button class="save-btn" id="saveBtn" onclick="showSaveModal()">Save as Collection</button>
</div>

<div id="grid" class="highlights-grid"></div>
<div id="emptyState" class="highlights-empty" style="display:none;">
  <p>No highlights available yet.</p>
  <p>Run a pipeline with quality scoring enabled to generate highlights.</p>
  <p><a href="/jobs">Go to Jobs</a></p>
</div>
<div class="highlights-summary" id="summary"></div>

<!-- Save modal -->
<div class="save-modal-overlay" id="saveModal">
  <div class="save-modal">
    <h3>Save as Collection</h3>
    <input type="text" id="saveNameInput" placeholder="Collection name">
    <div id="saveDuplicateWarning" style="display:none; color:var(--warning); font-size:13px; margin-bottom:12px;"></div>
    <div class="save-modal-actions">
      <button onclick="closeSaveModal()" style="background:var(--bg-secondary); color:var(--text-primary);">Cancel</button>
      <button id="replaceBtn" onclick="doSave('replace')" style="display:none; background:var(--warning); color:#fff;">Replace</button>
      <button onclick="doSave('new')" style="background:var(--accent); color:#fff;">Save</button>
    </div>
  </div>
</div>

<script>
var currentPhotos = [];
var currentFolders = [];
var currentFolderName = '';
var debounceTimer = null;
var existingCollections = [];

async function loadHighlights() {
  var params = new URLSearchParams();
  var folderSelect = document.getElementById('folderSelect');
  if (folderSelect.value) params.set('folder_id', folderSelect.value);
  params.set('count', document.getElementById('countSlider').value);
  params.set('max_per_species', document.getElementById('speciesSlider').value);
  params.set('min_quality', (parseInt(document.getElementById('qualitySlider').value) / 100).toFixed(2));

  var resp = await safeFetch('/api/highlights?' + params);
  var data = await resp.json();

  currentPhotos = data.photos;
  currentFolders = data.folders;

  // Populate folder dropdown (only on first load or if folders changed)
  if (folderSelect.options.length !== data.folders.length) {
    folderSelect.innerHTML = '';
    data.folders.forEach(function(f) {
      var opt = document.createElement('option');
      opt.value = f.id;
      opt.textContent = f.name + ' (' + f.photo_count + ')';
      folderSelect.appendChild(opt);
    });
  }

  currentFolderName = folderSelect.options[folderSelect.selectedIndex]
    ? folderSelect.options[folderSelect.selectedIndex].textContent.replace(/\s*\(\d+\)$/, '')
    : '';

  var grid = document.getElementById('grid');
  var emptyState = document.getElementById('emptyState');
  var summary = document.getElementById('summary');

  if (!data.folders.length) {
    grid.style.display = 'none';
    emptyState.style.display = 'block';
    summary.textContent = '';
    document.getElementById('controlsBar').style.display = 'none';
    return;
  }

  emptyState.style.display = 'none';
  grid.style.display = '';
  document.getElementById('controlsBar').style.display = '';

  // Update count slider max
  var countSlider = document.getElementById('countSlider');
  countSlider.max = Math.min(data.meta.eligible, 100);

  // Render grid
  grid.innerHTML = '';
  data.photos.forEach(function(p) {
    var card = document.createElement('div');
    card.className = 'highlights-card';
    card.onclick = function() { if (window.openLightbox) openLightbox(p); };
    var thumbSrc = p.thumb_path ? ('/thumbnails/' + p.thumb_path) : '/static/placeholder.png';
    card.innerHTML = '<img src="' + thumbSrc + '" alt="' + (p.filename || '') + '" loading="lazy">'
      + '<div class="card-info">'
      + '<span class="card-species">' + (p.species || 'Unidentified') + '</span>'
      + '<span class="card-quality">' + (p.quality_score != null ? p.quality_score.toFixed(2) : '—') + '</span>'
      + '</div>';
    grid.appendChild(card);
  });

  // Summary
  var speciesCount = Object.keys(data.meta.species_breakdown).length;
  summary.textContent = 'Showing ' + data.photos.length + ' highlights from ' + data.meta.total_in_folder
    + ' photos · Species: ' + speciesCount + ' · Avg quality: ' + data.meta.avg_quality;
}

function debounceLoad() {
  clearTimeout(debounceTimer);
  debounceTimer = setTimeout(loadHighlights, 300);
}

// Slider value display updates
document.getElementById('countSlider').addEventListener('input', function() {
  document.getElementById('countValue').textContent = this.value;
  debounceLoad();
});
document.getElementById('speciesSlider').addEventListener('input', function() {
  document.getElementById('speciesValue').textContent = this.value;
  debounceLoad();
});
document.getElementById('qualitySlider').addEventListener('input', function() {
  document.getElementById('qualityValue').textContent = (this.value / 100).toFixed(2);
  debounceLoad();
});
document.getElementById('folderSelect').addEventListener('change', loadHighlights);

// Save modal
async function showSaveModal() {
  document.getElementById('saveNameInput').value = 'Highlights - ' + currentFolderName;
  document.getElementById('saveDuplicateWarning').style.display = 'none';
  document.getElementById('replaceBtn').style.display = 'none';

  // Check for existing collection with same name
  var resp = await safeFetch('/api/collections');
  existingCollections = await resp.json();
  var proposed = document.getElementById('saveNameInput').value;
  var existing = existingCollections.find(function(c) { return c.name === proposed; });
  if (existing) {
    document.getElementById('saveDuplicateWarning').textContent =
      'A collection named "' + proposed + '" already exists.';
    document.getElementById('saveDuplicateWarning').style.display = 'block';
    document.getElementById('replaceBtn').style.display = '';
  }
  document.getElementById('saveModal').classList.add('open');
}

function closeSaveModal() {
  document.getElementById('saveModal').classList.remove('open');
}

async function doSave(mode) {
  var name = document.getElementById('saveNameInput').value.trim();
  if (!name) return;
  var photoIds = currentPhotos.map(function(p) { return p.id; });

  if (mode === 'replace') {
    var existing = existingCollections.find(function(c) { return c.name === name; });
    if (existing) {
      await safeFetch('/api/collections/' + existing.id, { method: 'DELETE' });
    }
  } else if (mode === 'new') {
    // If name already taken, append number
    var baseName = name;
    var counter = 2;
    var names = existingCollections.map(function(c) { return c.name; });
    while (names.indexOf(name) >= 0) {
      name = baseName + ' (' + counter + ')';
      counter++;
    }
  }

  await safeFetch('/api/highlights/save', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ photo_ids: photoIds, name: name }),
  });
  closeSaveModal();
}

// Initial load
loadHighlights();
</script>
</body>
</html>
```

**Step 2: Verify the page renders**

Run: `cd /Users/julius/git/vireo/.worktrees/highlights && python -m pytest vireo/tests/test_app.py -k "test_highlights" -v`
Expected: PASS (the existing tests from Task 4 still pass, confirming the template renders without errors)

**Step 3: Commit**

```bash
git add vireo/templates/highlights.html
git commit -m "feat(highlights): add highlights page template with controls and grid"
```

---

### Task 6: Add highlights link to navbar

**Files:**
- Modify: `vireo/templates/_navbar.html` (line ~811, between existing nav links)

**Step 1: Add the nav link**

In `vireo/templates/_navbar.html`, add the highlights link after the "Cull" link (line 811) and before "Browse" (line 812):

Find:
```html
  <a href="/cull" data-nav-id="cull">Cull</a>
  <a href="/browse" data-nav-id="browse">Browse</a>
```

Replace with:
```html
  <a href="/cull" data-nav-id="cull">Cull</a>
  <a href="/highlights" data-nav-id="highlights">Highlights</a>
  <a href="/browse" data-nav-id="browse">Browse</a>
```

**Step 2: Verify all existing tests still pass**

Run: `cd /Users/julius/git/vireo/.worktrees/highlights && python -m pytest tests/test_workspaces.py vireo/tests/test_db.py vireo/tests/test_app.py vireo/tests/test_photos_api.py vireo/tests/test_edits_api.py vireo/tests/test_jobs_api.py vireo/tests/test_darktable_api.py vireo/tests/test_config.py -q`
Expected: All pass

**Step 3: Commit**

```bash
git add vireo/templates/_navbar.html
git commit -m "feat(highlights): add Highlights link to navbar"
```

---

### Task 7: Full test suite verification

**Step 1: Run the full test suite**

Run: `cd /Users/julius/git/vireo/.worktrees/highlights && python -m pytest tests/test_workspaces.py vireo/tests/test_db.py vireo/tests/test_app.py vireo/tests/test_photos_api.py vireo/tests/test_edits_api.py vireo/tests/test_jobs_api.py vireo/tests/test_darktable_api.py vireo/tests/test_config.py vireo/tests/test_highlights.py -v`
Expected: All pass

**Step 2: If any failures, fix them and commit the fix**

**Step 3: Create PR**

```bash
cd /Users/julius/git/vireo/.worktrees/highlights
git push -u origin feat/highlights-page
gh pr create --title "feat: add Highlights page" --body "$(cat <<'EOF'
## Summary
- Adds a dedicated Highlights page (`/highlights`) that surfaces the best, most diverse photos from a pipeline run
- Interactive controls: folder picker, count slider, max-per-species slider, min quality threshold slider
- MMR-based selection reusing existing quality scores and DINO embeddings for diversity
- Save highlights as a static collection with duplicate detection (replace or create new)
- Empty state guides users to run a pipeline first

## New files
- `vireo/highlights.py` — Selection logic with per-species caps
- `vireo/tests/test_highlights.py` — Tests for selection logic
- `vireo/templates/highlights.html` — Page template

## Modified files
- `vireo/db.py` — Added `get_highlights_candidates()` and `get_folders_with_quality_data()`
- `vireo/app.py` — Added `/highlights`, `GET /api/highlights`, `POST /api/highlights/save` routes
- `vireo/templates/_navbar.html` — Added Highlights nav link

## Design doc
`docs/plans/2026-04-04-highlights-page-design.md`

## Test plan
- [ ] All existing tests pass
- [ ] New highlights selection tests pass
- [ ] New API route tests pass
- [ ] Page loads with empty state when no quality data exists
- [ ] Page loads and displays highlights when quality data exists
- [ ] Sliders update the grid after debounce
- [ ] Save as Collection creates a static collection
- [ ] Duplicate name detection works (replace vs create new)

🤖 Generated with [Claude Code](https://claude.com/claude-code)
EOF
)"
```
