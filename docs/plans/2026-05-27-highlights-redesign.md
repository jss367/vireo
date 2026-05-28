# Highlights Redesign Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Replace the flat MMR-based Highlights page with a species-bucketed view that uses classifier predictions as a fallback when no species has been accepted.

**Architecture:** Augment `get_highlights_candidates` to surface the highest-confidence non-rejected prediction per photo. Replace MMR selection in `/api/highlights` with Python-side bucketing keyed on `effective_species = accepted_species or (predicted if conf >= threshold)`. Rewrite the template as one row per species (sortable, expand-on-click), with a separate "Unidentified" section pinned at the bottom. Delete the MMR module.

**Tech Stack:** Flask, Jinja2, SQLite, vanilla JS. No new dependencies.

**Reference:** Design doc at `docs/plans/2026-05-27-highlights-redesign-design.md`.

---

## Pre-flight

We are already in a Conductor workspace on branch `highlights-page-redesign`. No worktree setup needed.

Verify before starting:

```bash
git branch --show-current
# Expected: highlights-page-redesign

git status --short
# Expected: clean (design doc already committed)
```

If anything is unexpected, stop and ask.

---

## Task 1: Surface the top-confidence non-rejected prediction per photo

**Files:**
- Modify: `vireo/db.py` (`Database.get_highlights_candidates`, ~line 6801)
- Modify: `vireo/tests/test_db.py` (existing highlights tests near line 6051; add new tests)

**Why:** Today's query only joins accepted-species keywords. The new page also needs to know "what would Vireo guess this photo is, if anything." We add a second LEFT JOIN that picks the highest-confidence prediction across the photo's detections, excluding predictions the user has rejected for the active workspace.

### Step 1.1 — Write the failing test (prediction columns present)

Append to `vireo/tests/test_db.py` after the existing highlights tests (after `test_get_highlights_candidates_workspace_wide_respects_min_quality_and_rejected`):

```python
def test_get_highlights_candidates_returns_predicted_species(tmp_path):
    """Photos with no accepted species but a classifier prediction
    expose ``predicted_species`` and ``predicted_confidence`` columns."""
    from db import Database
    db = Database(str(tmp_path / "test.db"))
    fid = db.add_folder('/p', name='p')
    pid = db.add_photo(folder_id=fid, filename='bird.jpg', extension='.jpg',
                       file_size=100, file_mtime=1.0)
    db.conn.execute("UPDATE photos SET quality_score = 0.6 WHERE id = ?", (pid,))
    did = db.conn.execute(
        "INSERT INTO detections (photo_id, detector_confidence) VALUES (?, 0.9)",
        (pid,),
    ).lastrowid
    db.conn.execute(
        "INSERT INTO predictions (detection_id, classifier_model, species, confidence) "
        "VALUES (?, 'test', 'ʻApapane', 0.82)",
        (did,),
    )
    db.conn.commit()

    rows = db.get_highlights_candidates(folder_id=fid, min_quality=0.0)
    assert len(rows) == 1
    assert rows[0]["predicted_species"] == "ʻApapane"
    assert abs(rows[0]["predicted_confidence"] - 0.82) < 1e-6
    # No accepted keyword → species is None
    assert rows[0]["species"] is None
```

### Step 1.2 — Run it and confirm failure

```bash
cd /Users/julius/conductor/workspaces/vireo/chengdu-v5
python -m pytest vireo/tests/test_db.py::test_get_highlights_candidates_returns_predicted_species -v
```

Expected: FAIL with `KeyError: 'predicted_species'` or similar.

### Step 1.3 — Implement: add the prediction LEFT JOIN

Edit `vireo/db.py`. In `get_highlights_candidates`, expand the SELECT list and add a new LEFT JOIN. Updated method body (replace the existing one — note the `ws` argument carried into the predictions subquery for the rejected-status filter, and the two new columns):

```python
def get_highlights_candidates(self, folder_id, min_quality=0.0):
    """Return photos eligible for highlights selection.

    When ``folder_id`` is an int, returns photos in that folder and its
    descendant folders. When ``folder_id`` is ``None``, returns photos
    across every folder visible in the active workspace.

    Each row carries:
      * ``species`` — accepted species keyword (NULL if none accepted)
      * ``predicted_species`` / ``predicted_confidence`` — top-confidence
        non-rejected prediction across the photo's detections (NULL if
        no usable prediction exists)

    Only photos with ``quality_score >= min_quality`` that are not
    user-rejected are returned, ordered by ``quality_score`` DESC.
    """
    ws = self._ws_id()
    if folder_id is None:
        folder_filter = ""
        folder_params = ()
    else:
        subtree = self.get_folder_subtree_ids(folder_id)
        placeholders = ",".join("?" for _ in subtree)
        folder_filter = f"AND p.folder_id IN ({placeholders})"
        folder_params = tuple(subtree)
    rows = self.conn.execute(
        f"""SELECT p.id, p.folder_id, p.filename, p.extension,
                  p.timestamp, p.width, p.height, p.rating, p.flag,
                  p.thumb_path, p.quality_score, p.subject_sharpness,
                  p.subject_size, p.sharpness, p.phash_crop,
                  p.dino_subject_embedding, p.dino_global_embedding,
                  bp.species,
                  tp.predicted_species,
                  tp.predicted_confidence
           FROM photos p
           JOIN workspace_folders wf ON wf.folder_id = p.folder_id
           JOIN folders f ON f.id = p.folder_id AND f.status IN ('ok', 'partial')
           LEFT JOIN (
               SELECT photo_id, name AS species FROM (
                   SELECT pk.photo_id, k.name,
                          ROW_NUMBER() OVER (
                              PARTITION BY pk.photo_id
                              ORDER BY pk.rowid DESC
                          ) AS rn
                   FROM photo_keywords pk
                   JOIN keywords k ON k.id = pk.keyword_id
                   WHERE k.is_species = 1
               ) WHERE rn = 1
           ) bp ON bp.photo_id = p.id
           LEFT JOIN (
               SELECT photo_id,
                      species AS predicted_species,
                      confidence AS predicted_confidence
               FROM (
                   SELECT d.photo_id, pr.species, pr.confidence,
                          ROW_NUMBER() OVER (
                              PARTITION BY d.photo_id
                              ORDER BY pr.confidence DESC, pr.id DESC
                          ) AS rn
                   FROM detections d
                   JOIN predictions pr ON pr.detection_id = d.id
                   LEFT JOIN prediction_review pr_rev
                     ON pr_rev.prediction_id = pr.id
                    AND pr_rev.workspace_id = ?
                   WHERE pr.species IS NOT NULL
                     AND COALESCE(pr_rev.status, 'pending') != 'rejected'
               ) WHERE rn = 1
           ) tp ON tp.photo_id = p.id
           WHERE wf.workspace_id = ?
             {folder_filter}
             AND p.quality_score IS NOT NULL
             AND p.quality_score >= ?
             AND p.flag != 'rejected'
           ORDER BY p.quality_score DESC""",
        (ws, ws, *folder_params, min_quality),
    ).fetchall()
    return rows
```

Note the parameter order: `(ws, ws, *folder_params, min_quality)` — first `ws` for the predictions subquery's `prediction_review` join, second `ws` for the outer `workspace_folders` filter.

### Step 1.4 — Run the new test (now passing) and the existing highlights tests (still passing)

```bash
python -m pytest vireo/tests/test_db.py -k highlights_candidates -v
```

Expected: every test passes, including the seven pre-existing ones and the new prediction test.

### Step 1.5 — Add two more tests covering edge cases

Append:

```python
def test_get_highlights_candidates_predicted_picks_highest_confidence(tmp_path):
    """When a photo has multiple detections with different predictions,
    the highest-confidence one is returned."""
    from db import Database
    db = Database(str(tmp_path / "test.db"))
    fid = db.add_folder('/p', name='p')
    pid = db.add_photo(folder_id=fid, filename='two.jpg', extension='.jpg',
                       file_size=100, file_mtime=1.0)
    db.conn.execute("UPDATE photos SET quality_score = 0.5 WHERE id = ?", (pid,))
    d1 = db.conn.execute(
        "INSERT INTO detections (photo_id, detector_confidence) VALUES (?, 0.9)",
        (pid,),
    ).lastrowid
    d2 = db.conn.execute(
        "INSERT INTO detections (photo_id, detector_confidence) VALUES (?, 0.9)",
        (pid,),
    ).lastrowid
    db.conn.execute(
        "INSERT INTO predictions (detection_id, classifier_model, species, confidence) "
        "VALUES (?, 'm', 'Boring Bird', 0.40)",
        (d1,),
    )
    db.conn.execute(
        "INSERT INTO predictions (detection_id, classifier_model, species, confidence) "
        "VALUES (?, 'm', 'Cool Bird', 0.90)",
        (d2,),
    )
    db.conn.commit()

    rows = db.get_highlights_candidates(folder_id=fid, min_quality=0.0)
    assert rows[0]["predicted_species"] == "Cool Bird"
    assert abs(rows[0]["predicted_confidence"] - 0.90) < 1e-6


def test_get_highlights_candidates_predicted_excludes_rejected(tmp_path):
    """Predictions the user rejected in the active workspace do not
    appear as the fallback species."""
    from db import Database
    db = Database(str(tmp_path / "test.db"))
    fid = db.add_folder('/p', name='p')
    pid = db.add_photo(folder_id=fid, filename='r.jpg', extension='.jpg',
                       file_size=100, file_mtime=1.0)
    db.conn.execute("UPDATE photos SET quality_score = 0.5 WHERE id = ?", (pid,))
    did = db.conn.execute(
        "INSERT INTO detections (photo_id, detector_confidence) VALUES (?, 0.9)",
        (pid,),
    ).lastrowid
    pred_id = db.conn.execute(
        "INSERT INTO predictions (detection_id, classifier_model, species, confidence) "
        "VALUES (?, 'm', 'Wrong Bird', 0.95)",
        (did,),
    ).lastrowid
    db.conn.execute(
        "INSERT INTO prediction_review (prediction_id, workspace_id, status) "
        "VALUES (?, ?, 'rejected')",
        (pred_id, db._ws_id()),
    )
    db.conn.commit()

    rows = db.get_highlights_candidates(folder_id=fid, min_quality=0.0)
    assert rows[0]["predicted_species"] is None
    assert rows[0]["predicted_confidence"] is None
```

Run them:

```bash
python -m pytest vireo/tests/test_db.py -k highlights_candidates -v
```

Expected: all pass.

### Step 1.6 — Commit

```bash
git add vireo/db.py vireo/tests/test_db.py
git commit -m "$(cat <<'EOF'
db: surface top non-rejected prediction in get_highlights_candidates

Adds predicted_species + predicted_confidence to each row, picking the
highest-confidence prediction across a photo's detections and excluding
predictions the user has rejected in the active workspace. Sets up the
species-bucketed highlights view.

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>
EOF
)"
```

---

## Task 2: Rewrite `/api/highlights` as a species-bucketed endpoint

**Files:**
- Modify: `vireo/app.py` (`api_highlights`, ~line 3842)
- Modify: `vireo/app.py` (drop `from highlights import select_highlights` at line 37)
- Modify: `vireo/tests/test_app.py` (rewrite highlights API tests starting near line 3916)

**Why:** The endpoint stops doing MMR selection and instead bucketizes photos by their effective species (accepted > prediction-above-threshold > Unidentified) and returns the new response shape from the design doc.

### Step 2.1 — Update the existing API tests for the new response shape

Replace `test_highlights_get_empty`, `test_highlights_get_with_data`, `test_highlights_scope_workspace_blends_folders`, `test_highlights_scope_workspace_isolates_other_workspaces`, and `test_highlights_folder_scope_still_works` in `vireo/tests/test_app.py`. The `test_highlights_save` test stays as-is. Replace with:

```python
def test_highlights_get_empty(app_and_db):
    """GET /api/highlights returns empty buckets when no quality data exists."""
    app, _ = app_and_db
    client = app.test_client()
    resp = client.get("/api/highlights")
    assert resp.status_code == 200
    data = resp.get_json()
    assert data["buckets"] == []
    assert data["unidentified"]["photo_count"] == 0
    assert data["folders"] == []
    assert data["meta"]["eligible"] == 0


def test_highlights_buckets_by_accepted_species(app_and_db):
    """Photos with accepted species keywords populate species buckets."""
    app, db = app_and_db
    client = app.test_client()
    fid = db.conn.execute(
        "INSERT INTO folders (path, name, status) VALUES ('/b', 'b', 'ok')"
    ).lastrowid
    db.conn.execute(
        "INSERT INTO workspace_folders (workspace_id, folder_id) VALUES (?, ?)",
        (db._ws_id(), fid),
    )
    apapane_kw = db.conn.execute(
        "INSERT INTO keywords (name, type, is_species) VALUES ('ʻApapane', 'taxonomy', 1)"
    ).lastrowid
    for i, q in enumerate([0.9, 0.7, 0.5]):
        pid = db.conn.execute(
            "INSERT INTO photos (folder_id, filename, quality_score, flag) "
            "VALUES (?, ?, ?, 'none')",
            (fid, f"a{i}.jpg", q),
        ).lastrowid
        db.conn.execute(
            "INSERT INTO photo_keywords (photo_id, keyword_id) VALUES (?, ?)",
            (pid, apapane_kw),
        )
    db.conn.commit()

    resp = client.get(f"/api/highlights?folder_id={fid}")
    assert resp.status_code == 200
    data = resp.get_json()
    assert len(data["buckets"]) == 1
    bucket = data["buckets"][0]
    assert bucket["species"] == "ʻApapane"
    assert bucket["is_accepted"] is True
    assert bucket["photo_count"] == 3
    assert bucket["best_quality"] == 0.9
    # Photos ordered by quality_score desc
    qs = [p["quality_score"] for p in bucket["photos"]]
    assert qs == sorted(qs, reverse=True)


def test_highlights_predictions_above_threshold_populate_buckets(app_and_db):
    """Predictions at or above confidence_threshold count as the photo's species."""
    app, db = app_and_db
    client = app.test_client()
    fid = db.conn.execute(
        "INSERT INTO folders (path, name, status) VALUES ('/p', 'p', 'ok')"
    ).lastrowid
    db.conn.execute(
        "INSERT INTO workspace_folders (workspace_id, folder_id) VALUES (?, ?)",
        (db._ws_id(), fid),
    )
    pid = db.conn.execute(
        "INSERT INTO photos (folder_id, filename, quality_score, flag) "
        "VALUES (?, 'p.jpg', 0.6, 'none')",
        (fid,),
    ).lastrowid
    did = db.conn.execute(
        "INSERT INTO detections (photo_id, detector_confidence) VALUES (?, 0.9)",
        (pid,),
    ).lastrowid
    db.conn.execute(
        "INSERT INTO predictions (detection_id, classifier_model, species, confidence) "
        "VALUES (?, 'm', 'ʻIʻiwi', 0.82)",
        (did,),
    )
    db.conn.commit()

    # Threshold 0.70 — prediction wins, populates species bucket
    resp = client.get(f"/api/highlights?folder_id={fid}&confidence_threshold=0.70")
    data = resp.get_json()
    assert len(data["buckets"]) == 1
    assert data["buckets"][0]["species"] == "ʻIʻiwi"
    assert data["buckets"][0]["is_accepted"] is False
    assert data["unidentified"]["photo_count"] == 0

    # Threshold 0.90 — prediction below threshold, photo falls to Unidentified
    resp = client.get(f"/api/highlights?folder_id={fid}&confidence_threshold=0.90")
    data = resp.get_json()
    assert data["buckets"] == []
    assert data["unidentified"]["photo_count"] == 1


def test_highlights_accepted_species_wins_over_higher_confidence_prediction(app_and_db):
    """Manual species tag is authoritative even when a high-confidence
    prediction disagrees."""
    app, db = app_and_db
    client = app.test_client()
    fid = db.conn.execute(
        "INSERT INTO folders (path, name, status) VALUES ('/c', 'c', 'ok')"
    ).lastrowid
    db.conn.execute(
        "INSERT INTO workspace_folders (workspace_id, folder_id) VALUES (?, ?)",
        (db._ws_id(), fid),
    )
    accepted_kw = db.conn.execute(
        "INSERT INTO keywords (name, type, is_species) VALUES ('Real Bird', 'taxonomy', 1)"
    ).lastrowid
    pid = db.conn.execute(
        "INSERT INTO photos (folder_id, filename, quality_score, flag) "
        "VALUES (?, 'x.jpg', 0.7, 'none')",
        (fid,),
    ).lastrowid
    db.conn.execute(
        "INSERT INTO photo_keywords (photo_id, keyword_id) VALUES (?, ?)",
        (pid, accepted_kw),
    )
    did = db.conn.execute(
        "INSERT INTO detections (photo_id, detector_confidence) VALUES (?, 0.9)",
        (pid,),
    ).lastrowid
    db.conn.execute(
        "INSERT INTO predictions (detection_id, classifier_model, species, confidence) "
        "VALUES (?, 'm', 'Wrong Bird', 0.99)",
        (did,),
    )
    db.conn.commit()

    resp = client.get(f"/api/highlights?folder_id={fid}&confidence_threshold=0.5")
    data = resp.get_json()
    assert len(data["buckets"]) == 1
    assert data["buckets"][0]["species"] == "Real Bird"
    assert data["buckets"][0]["is_accepted"] is True


def test_highlights_scope_workspace_blends_folders(app_and_db):
    """scope=workspace blends candidates across every folder in the
    active workspace (matches existing folder-scope behavior)."""
    app, db = app_and_db
    client = app.test_client()
    apapane_kw = db.conn.execute(
        "INSERT INTO keywords (name, type, is_species) VALUES ('ʻApapane', 'taxonomy', 1)"
    ).lastrowid
    for fname in ("2024-01-15", "2024-01-16"):
        fid = db.conn.execute(
            "INSERT INTO folders (path, name, status) VALUES (?, ?, 'ok')",
            (f"/shoot/{fname}", fname),
        ).lastrowid
        db.conn.execute(
            "INSERT INTO workspace_folders (workspace_id, folder_id) VALUES (?, ?)",
            (db._ws_id(), fid),
        )
        pid = db.conn.execute(
            "INSERT INTO photos (folder_id, filename, quality_score, flag) "
            "VALUES (?, ?, 0.8, 'none')",
            (fid, f"{fname}.jpg"),
        ).lastrowid
        db.conn.execute(
            "INSERT INTO photo_keywords (photo_id, keyword_id) VALUES (?, ?)",
            (pid, apapane_kw),
        )
    db.conn.commit()

    resp = client.get("/api/highlights?scope=workspace")
    data = resp.get_json()
    assert len(data["buckets"]) == 1
    assert data["buckets"][0]["photo_count"] == 2
```

The existing `test_highlights_save` stays as-is; it only depends on `/api/highlights/save` which is unchanged.

### Step 2.2 — Run the tests; confirm they fail against the current endpoint

```bash
python -m pytest vireo/tests/test_app.py -k highlights -v
```

Expected: all the rewritten tests FAIL (response shape mismatch — `KeyError: 'buckets'` etc.). `test_highlights_save` and `test_highlights_page` still pass.

### Step 2.3 — Implement the new endpoint

In `vireo/app.py`:

1. Delete the import on line 37: `from highlights import select_highlights`.
2. Replace `api_highlights` (around line 3842 through line 3906) with:

```python
@app.route("/api/highlights")
def api_highlights():
    db = _get_db()

    folders = db.get_folders_with_quality_data()

    scope = request.args.get("scope", "folder")
    folder_id = request.args.get("folder_id", type=int)
    if scope == "workspace":
        folder_id = None
    elif folder_id is None and folders:
        folder_id = folders[0]["id"]  # Most recent

    min_quality = request.args.get("min_quality", 0.0, type=float)
    confidence_threshold = request.args.get(
        "confidence_threshold", 0.70, type=float
    )

    candidates = db.get_highlights_candidates(folder_id, min_quality=min_quality)
    total_in_workspace = db.count_filtered_photos(folder_id=folder_id)

    # Resolve effective species: accepted wins, then prediction above
    # threshold, otherwise Unidentified.
    bucket_map = {}  # species name -> {is_accepted: bool, photos: list}
    unidentified_photos = []

    for row in candidates:
        r = dict(row)
        accepted = r.get("species")
        if accepted:
            species = accepted
            is_accepted = True
        elif (r.get("predicted_species")
              and r.get("predicted_confidence") is not None
              and r["predicted_confidence"] >= confidence_threshold):
            species = r["predicted_species"]
            is_accepted = False
        else:
            species = None
            is_accepted = False

        photo = {
            "id": r["id"],
            "filename": r["filename"],
            "quality_score": r["quality_score"],
            "has_accepted_species": accepted is not None,
        }

        if species is None:
            unidentified_photos.append(photo)
        else:
            entry = bucket_map.setdefault(
                species, {"is_accepted": False, "photos": []}
            )
            # is_accepted is True if ANY photo in this bucket is accepted.
            # (Used by UI to render an "accepted" badge on the row.)
            entry["is_accepted"] = entry["is_accepted"] or is_accepted
            entry["photos"].append(photo)

    # candidates is already ordered by quality_score desc, so per-bucket
    # photos inherit that order without resorting.

    buckets = []
    for species, entry in bucket_map.items():
        photos = entry["photos"]
        buckets.append({
            "species": species,
            "is_accepted": entry["is_accepted"],
            "photo_count": len(photos),
            "best_quality": photos[0]["quality_score"] if photos else None,
            "photos": photos,
        })

    return jsonify({
        "buckets": buckets,
        "unidentified": {
            "photo_count": len(unidentified_photos),
            "photos": unidentified_photos,
        },
        "folders": [
            {"id": f["id"], "name": f["name"], "photo_count": f["photo_count"]}
            for f in folders
        ],
        "meta": {
            "total_in_workspace": total_in_workspace,
            "eligible": len(candidates),
        },
        "scope": "workspace" if folder_id is None else "folder",
    })
```

### Step 2.4 — Run tests; confirm they pass

```bash
python -m pytest vireo/tests/test_app.py -k highlights -v
```

Expected: all pass, including `test_highlights_save` and `test_highlights_page`.

### Step 2.5 — Run the full test suite to catch any other consumers of the old API

```bash
python -m pytest tests/test_workspaces.py vireo/tests/test_db.py vireo/tests/test_app.py vireo/tests/test_photos_api.py vireo/tests/test_edits_api.py vireo/tests/test_jobs_api.py vireo/tests/test_darktable_api.py vireo/tests/test_config.py -v
```

Expected: all pass (modulo any pre-existing failures noted in [[project_preexisting_test_failures]] — verify failures predate this branch with `git stash && pytest; git stash pop`).

### Step 2.6 — Commit

```bash
git add vireo/app.py vireo/tests/test_app.py
git commit -m "$(cat <<'EOF'
api/highlights: species-bucketed response with prediction fallback

Replaces MMR-based selection with bucketing by effective species
(accepted > prediction at/above threshold > Unidentified). Adds
confidence_threshold query param (default 0.70). Drops the count
parameter. Response shape changes; only consumer is the template.

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>
EOF
)"
```

---

## Task 3: Rewrite the Highlights template

**Files:**
- Modify: `vireo/templates/highlights.html` (complete rewrite)

**Why:** The template is the only consumer of the new API shape. It needs new controls, species rows with expand, an Unidentified section, and client-side sort.

### Step 3.1 — Replace the template

Overwrite `vireo/templates/highlights.html` with:

```html
<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<link rel="icon" type="image/png" href="/favicon.ico">
<link rel="apple-touch-icon" href="/static/apple-touch-icon.png">
<link rel="stylesheet" href="/static/vireo-base.css">
<title>Vireo - Highlights</title>
<style>
  .highlights-controls { display:flex; align-items:center; gap:20px; padding:12px 24px; background:var(--bg-secondary); border-bottom:1px solid var(--border-primary); flex-wrap:wrap; }
  .control-group { display:flex; align-items:center; gap:8px; }
  .control-group label { font-size:13px; color:var(--text-secondary); white-space:nowrap; }
  .control-group input[type=range] { width:120px; }
  .control-group select { font-size:13px; padding:4px 8px; background:var(--bg-input); color:var(--text-primary); border:1px solid var(--border-secondary); border-radius:4px; }
  .control-value { font-size:13px; color:var(--text-primary); min-width:36px; }
  .save-btn { margin-left:auto; padding:6px 16px; background:var(--accent); color:var(--accent-text); border:none; border-radius:4px; cursor:pointer; font-size:13px; }
  .save-btn:hover { opacity:0.9; }

  .bucket { padding:16px 24px; border-bottom:1px solid var(--border-primary); }
  .bucket-header { display:flex; align-items:baseline; justify-content:space-between; margin-bottom:10px; }
  .bucket-title { font-size:16px; color:var(--text-primary); font-weight:600; }
  .bucket-meta { font-size:13px; color:var(--text-secondary); }
  .bucket-badge { display:inline-block; margin-left:8px; font-size:10px; padding:2px 6px; border-radius:8px; background:var(--bg-tertiary); color:var(--text-secondary); text-transform:uppercase; letter-spacing:0.5px; }
  .bucket-badge.predicted { background:var(--bg-tertiary); color:var(--text-secondary); }
  .bucket-badge.accepted { background:var(--accent); color:var(--accent-text); }
  .bucket-strip { display:grid; grid-template-columns:repeat(auto-fill, minmax(180px, 1fr)); gap:10px; }
  .highlights-card { position:relative; border-radius:6px; overflow:hidden; background:var(--bg-secondary); cursor:pointer; border:1px solid var(--border-primary); }
  .highlights-card:hover { border-color:var(--accent); }
  .highlights-card img { width:100%; aspect-ratio:3/2; object-fit:cover; display:block; background:var(--bg-tertiary); }
  .card-info { padding:4px 8px; font-size:11px; color:var(--text-secondary); text-align:right; }
  .bucket-expand { margin-top:8px; font-size:13px; color:var(--accent); cursor:pointer; background:none; border:none; padding:0; }
  .bucket-expand:hover { text-decoration:underline; }

  .unid-divider { padding:20px 24px 8px; font-size:11px; color:var(--text-secondary); text-transform:uppercase; letter-spacing:1px; border-top:2px solid var(--border-primary); margin-top:8px; }

  .highlights-empty { text-align:center; padding:80px 24px; color:var(--text-secondary); }
  .highlights-empty a { color:var(--accent); }
  .highlights-meta { padding:8px 24px; font-size:12px; color:var(--text-secondary); }

  /* Save modal (unchanged) */
  .save-modal-overlay { display:none; position:fixed; inset:0; background:rgba(0,0,0,0.5); z-index:500; align-items:center; justify-content:center; }
  .save-modal-overlay.open { display:flex; }
  .save-modal { background:var(--bg-primary); border:1px solid var(--border-primary); border-radius:8px; padding:24px; min-width:360px; }
  .save-modal h3 { margin:0 0 16px; font-size:16px; color:var(--text-primary); }
  .save-modal input[type=text] { width:100%; padding:8px; font-size:14px; border:1px solid var(--border-secondary); border-radius:4px; box-sizing:border-box; margin-bottom:12px; background:var(--bg-input); color:var(--text-primary); }
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
    <label for="qualitySlider">Min quality</label>
    <input type="range" id="qualitySlider" min="0" max="100" value="0" step="5">
    <span class="control-value" id="qualityValue">0.00</span>
  </div>
  <div class="control-group">
    <label for="confidenceSlider">Auto-ID confidence</label>
    <input type="range" id="confidenceSlider" min="50" max="95" value="70" step="5">
    <span class="control-value" id="confidenceValue">0.70</span>
  </div>
  <div class="control-group">
    <label for="perRowSlider">Per row</label>
    <input type="range" id="perRowSlider" min="1" max="20" value="5">
    <span class="control-value" id="perRowValue">5</span>
  </div>
  <div class="control-group">
    <label for="sortSelect">Sort</label>
    <select id="sortSelect">
      <option value="fewest">Fewest photos</option>
      <option value="most">Most photos</option>
      <option value="best">Best photo first</option>
      <option value="worst">Worst photo first</option>
    </select>
  </div>
  <button class="save-btn" id="saveBtn" onclick="showSaveModal()">Save as Collection</button>
</div>

<div class="highlights-meta" id="meta"></div>

<div id="content"></div>

<div id="emptyState" class="highlights-empty" style="display:none;">
  <p>No highlights available yet.</p>
  <p>Run a pipeline with quality scoring enabled to generate highlights.</p>
  <p><a href="/jobs">Go to Jobs</a></p>
</div>

<!-- Save modal -->
<div class="save-modal-overlay" id="saveModal">
  <div class="save-modal">
    <h3>Save as Collection</h3>
    <input type="text" id="saveNameInput" placeholder="Collection name">
    <div id="saveDuplicateWarning" style="display:none; color:var(--warning); font-size:13px; margin-bottom:12px;"></div>
    <div class="save-modal-actions">
      <button onclick="closeSaveModal()" style="background:var(--bg-secondary); color:var(--text-primary);">Cancel</button>
      <button id="replaceBtn" onclick="doSave('replace')" style="display:none; background:var(--warning); color:#fff;">Replace</button>
      <button onclick="doSave('new')" style="background:var(--accent); color:var(--accent-text);">Save</button>
    </div>
  </div>
</div>

<script>
var WORKSPACE_SCOPE = '__workspace__';
var currentData = null;          // last API response
var expandedBuckets = new Set(); // species names whose row is expanded
var unidentifiedExpanded = false;
var currentFolderName = '';
var debounceTimer = null;
var existingCollections = [];

function escapeAttr(s) {
  if (s === null || s === undefined) return '';
  return String(s).replace(/&/g, '&amp;').replace(/"/g, '&quot;').replace(/'/g, '&#39;').replace(/</g, '&lt;').replace(/>/g, '&gt;');
}

async function loadHighlights() {
  var params = new URLSearchParams();
  var folderSelect = document.getElementById('folderSelect');
  var firstLoad = folderSelect.options.length === 0;
  if (folderSelect.value === WORKSPACE_SCOPE || firstLoad) {
    params.set('scope', 'workspace');
  } else if (folderSelect.value) {
    params.set('folder_id', folderSelect.value);
  }
  params.set('min_quality', (parseInt(document.getElementById('qualitySlider').value) / 100).toFixed(2));
  params.set('confidence_threshold', (parseInt(document.getElementById('confidenceSlider').value) / 100).toFixed(2));

  var data = await safeFetch('/api/highlights?' + params);
  currentData = data;

  // Populate folder dropdown on first load.
  if (!folderSelect.options.length) {
    var wsOpt = document.createElement('option');
    wsOpt.value = WORKSPACE_SCOPE;
    wsOpt.textContent = 'All folders in this workspace';
    folderSelect.appendChild(wsOpt);
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

  render();
}

function sortedBuckets() {
  var sort = document.getElementById('sortSelect').value;
  var buckets = (currentData.buckets || []).slice();
  buckets.sort(function(a, b) {
    if (sort === 'most') return b.photo_count - a.photo_count;
    if (sort === 'best') return b.best_quality - a.best_quality;
    if (sort === 'worst') return a.best_quality - b.best_quality;
    return a.photo_count - b.photo_count; // 'fewest' default
  });
  return buckets;
}

function renderCard(p) {
  var thumbSrc = '/thumbnails/' + p.id + '.jpg';
  return '<div class="highlights-card" data-photo-id="' + p.id + '">'
    + '<img src="' + thumbSrc + '" alt="' + escapeAttr(p.filename) + '" loading="lazy">'
    + '<div class="card-info">' + (p.quality_score != null ? p.quality_score.toFixed(2) : '—') + '</div>'
    + '</div>';
}

function attachCardClicks(container, photoSet) {
  // photoSet is the full list of photos in this bucket — used as the
  // lightbox navigation set so users can flip through every photo of
  // the species, not just the visible ones.
  container.querySelectorAll('.highlights-card').forEach(function(card) {
    card.addEventListener('click', function() {
      var pid = parseInt(card.getAttribute('data-photo-id'));
      var photo = photoSet.find(function(p) { return p.id === pid; });
      if (window.openLightbox && photo) {
        openLightbox(pid, photo.filename, photoSet);
      }
    });
  });
}

function renderBucket(bucket) {
  var perRow = parseInt(document.getElementById('perRowSlider').value);
  var expanded = expandedBuckets.has(bucket.species);
  var visible = expanded ? bucket.photos : bucket.photos.slice(0, perRow);
  var remaining = bucket.photos.length - visible.length;
  var badge = bucket.is_accepted
    ? '<span class="bucket-badge accepted">Confirmed</span>'
    : '<span class="bucket-badge predicted">Predicted</span>';
  var meta = bucket.photo_count + ' photo' + (bucket.photo_count === 1 ? '' : 's')
    + ' · best ' + bucket.best_quality.toFixed(2);
  var expandBtn = '';
  if (bucket.photos.length > perRow) {
    expandBtn = '<button class="bucket-expand" data-species="'
      + escapeAttr(bucket.species) + '">'
      + (expanded ? 'Show fewer' : '+ ' + remaining + ' more')
      + '</button>';
  }
  return '<section class="bucket">'
    + '<div class="bucket-header">'
    + '<div class="bucket-title">' + escapeAttr(bucket.species) + badge + '</div>'
    + '<div class="bucket-meta">' + meta + '</div>'
    + '</div>'
    + '<div class="bucket-strip">' + visible.map(renderCard).join('') + '</div>'
    + expandBtn
    + '</section>';
}

function renderUnidentified() {
  var unid = currentData.unidentified;
  if (!unid || !unid.photo_count) return '';
  var perRow = parseInt(document.getElementById('perRowSlider').value);
  var visible = unidentifiedExpanded ? unid.photos : unid.photos.slice(0, perRow);
  var remaining = unid.photo_count - visible.length;
  var expandBtn = '';
  if (unid.photo_count > perRow) {
    expandBtn = '<button class="bucket-expand" data-unid="1">'
      + (unidentifiedExpanded ? 'Show fewer' : '+ ' + remaining + ' more')
      + '</button>';
  }
  return '<div class="unid-divider">Unidentified — Vireo couldn\'t ID these</div>'
    + '<section class="bucket">'
    + '<div class="bucket-header">'
    + '<div class="bucket-title">Unidentified</div>'
    + '<div class="bucket-meta">' + unid.photo_count + ' photos</div>'
    + '</div>'
    + '<div class="bucket-strip">' + visible.map(renderCard).join('') + '</div>'
    + expandBtn
    + '</section>';
}

function render() {
  var content = document.getElementById('content');
  var empty = document.getElementById('emptyState');
  var meta = document.getElementById('meta');
  var controls = document.getElementById('controlsBar');

  if (!currentData.folders.length) {
    content.innerHTML = '';
    empty.style.display = 'block';
    meta.textContent = '';
    controls.style.display = 'none';
    return;
  }
  empty.style.display = 'none';
  controls.style.display = '';

  var buckets = sortedBuckets();
  meta.textContent = buckets.length + ' species · '
    + currentData.meta.eligible + ' of '
    + currentData.meta.total_in_workspace + ' photos scored';

  var html = buckets.map(renderBucket).join('') + renderUnidentified();
  content.innerHTML = html || '<div class="highlights-empty"><p>Nothing matches these filters.</p></div>';

  // Wire up clicks for cards and expand buttons.
  buckets.forEach(function(b) {
    var section = content.querySelector('section.bucket .bucket-title');
    // Per-bucket click wiring done by section via per-card photo set
  });
  // Attach card clicks: pick the right photo set per section by walking sections in DOM order.
  var sections = content.querySelectorAll('section.bucket');
  sections.forEach(function(section, idx) {
    var photoSet;
    if (idx < buckets.length) {
      photoSet = buckets[idx].photos;
    } else {
      photoSet = currentData.unidentified.photos;
    }
    attachCardClicks(section, photoSet);
  });

  // Expand buttons
  content.querySelectorAll('.bucket-expand').forEach(function(btn) {
    btn.addEventListener('click', function() {
      if (btn.hasAttribute('data-unid')) {
        unidentifiedExpanded = !unidentifiedExpanded;
      } else {
        var sp = btn.getAttribute('data-species');
        if (expandedBuckets.has(sp)) expandedBuckets.delete(sp);
        else expandedBuckets.add(sp);
      }
      render();
    });
  });
}

function debounceLoad() {
  clearTimeout(debounceTimer);
  debounceTimer = setTimeout(loadHighlights, 300);
}

document.getElementById('qualitySlider').addEventListener('input', function() {
  document.getElementById('qualityValue').textContent = (this.value / 100).toFixed(2);
  debounceLoad();
});
document.getElementById('confidenceSlider').addEventListener('input', function() {
  document.getElementById('confidenceValue').textContent = (this.value / 100).toFixed(2);
  debounceLoad();
});
document.getElementById('perRowSlider').addEventListener('input', function() {
  document.getElementById('perRowValue').textContent = this.value;
  if (currentData) render();
});
document.getElementById('sortSelect').addEventListener('change', function() {
  if (currentData) render();
});
document.getElementById('folderSelect').addEventListener('change', loadHighlights);

// Save modal
async function showSaveModal() {
  document.getElementById('saveNameInput').value = 'Highlights - ' + currentFolderName;
  document.getElementById('saveDuplicateWarning').style.display = 'none';
  document.getElementById('replaceBtn').style.display = 'none';
  existingCollections = await safeFetch('/api/collections');
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

function collectVisiblePhotoIds() {
  // "What's currently visible on the page" = the photos rendered in
  // each bucket (respecting expand state) plus unidentified if shown.
  var ids = [];
  var perRow = parseInt(document.getElementById('perRowSlider').value);
  sortedBuckets().forEach(function(b) {
    var visible = expandedBuckets.has(b.species) ? b.photos : b.photos.slice(0, perRow);
    visible.forEach(function(p) { ids.push(p.id); });
  });
  if (currentData.unidentified && currentData.unidentified.photo_count) {
    var u = currentData.unidentified.photos;
    var v = unidentifiedExpanded ? u : u.slice(0, perRow);
    v.forEach(function(p) { ids.push(p.id); });
  }
  return ids;
}

async function doSave(mode) {
  var name = document.getElementById('saveNameInput').value.trim();
  if (!name) return;
  var photoIds = collectVisiblePhotoIds();

  if (mode === 'replace') {
    var existing = existingCollections.find(function(c) { return c.name === name; });
    if (existing) {
      await safeFetch('/api/collections/' + existing.id, { method: 'DELETE' });
    }
  } else if (mode === 'new') {
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

loadHighlights();
</script>
</body>
</html>
```

### Step 3.2 — Sanity-check via Flask test client

Add a smoke test to `vireo/tests/test_app.py` that confirms the template still renders:

```python
def test_highlights_page_renders_after_redesign(app_and_db):
    """The page template still renders against the new API shape."""
    app, _ = app_and_db
    client = app.test_client()
    resp = client.get("/highlights")
    assert resp.status_code == 200
    assert b"Auto-ID confidence" in resp.data
    assert b"Per row" in resp.data
```

Run:

```bash
python -m pytest vireo/tests/test_app.py::test_highlights_page_renders_after_redesign -v
```

Expected: PASS.

### Step 3.3 — Commit

```bash
git add vireo/templates/highlights.html vireo/tests/test_app.py
git commit -m "$(cat <<'EOF'
highlights: species-bucketed view with expand and client-side sort

Each species becomes a row with the top photos shown collapsed; click
"+N more" to expand. Unidentified gets its own section below the
species rows. Controls: folder, min quality, auto-ID confidence
threshold, per-row count, sort (fewest/most/best/worst).

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>
EOF
)"
```

---

## Task 4: Manual UI verification in the browser

Per the user-first testing rule ([[feedback_user_first_testing]]), drive a real browser before claiming the page works on real data.

### Step 4.1 — Start Vireo

```bash
python vireo/app.py --db ~/.vireo/vireo.db --port 8080 &
```

(Run in background; will be killed at the end of the task.)

### Step 4.2 — Walk through the Hawaii workspace

Open `http://localhost:8080/highlights` in a browser, with the active workspace already set to **Hawaii** (the workspace switcher is in the navbar). Verify:

1. The default sort puts **rare species first** — there should be species with 1–5 photos at the top, not Saffron Finch (800).
2. **ʻApapane appears** as its own row, with a "Predicted" badge (no accepted-species tag in Hawaii based on the earlier diagnosis).
3. The **Unidentified section** is at the bottom with a clear divider, and its photo count is non-trivial (several thousand for Hawaii).
4. Dragging **Auto-ID confidence to 0.95** drops ʻApapane out of buckets (its predictions average ~0.56, max ~0.82) and grows the Unidentified count.
5. Dragging it back to **0.50** populates many more species buckets.
6. The **Sort dropdown** reorders rows instantly without a network request.
7. Clicking **+N more** expands a row inline; clicking again ("Show fewer") collapses it.
8. Clicking a **thumbnail** opens the lightbox; navigating left/right walks every photo in that species bucket (not just the visible ones).
9. **Save as Collection** still works — produces a collection containing exactly the visible photos.

If any of these fail, stop and surface the failure before continuing.

### Step 4.3 — Stop Vireo and report

```bash
pkill -f "python vireo/app.py"
```

Report a short note in the PR description summarizing what was checked in the browser.

No commit for this step.

---

## Task 5: Delete dead MMR code

**Files:**
- Delete: `vireo/highlights.py`
- Delete: `vireo/tests/test_highlights.py`

**Why:** Task 2 stopped importing `select_highlights`. The module and its tests are dead weight now. `vireo/selection.py` (used by encounter selection) stays.

### Step 5.1 — Confirm no other consumers

```bash
```

Use the Grep tool with pattern `from highlights import|import highlights|select_highlights` across the repo. The only remaining matches should be the file `vireo/highlights.py` itself and the test file `vireo/tests/test_highlights.py`.

If anything else still references either, stop and reconsider.

### Step 5.2 — Delete

```bash
git rm vireo/highlights.py vireo/tests/test_highlights.py
```

### Step 5.3 — Verify tests still pass

```bash
python -m pytest tests/test_workspaces.py vireo/tests/test_db.py vireo/tests/test_app.py vireo/tests/test_photos_api.py vireo/tests/test_edits_api.py vireo/tests/test_jobs_api.py vireo/tests/test_darktable_api.py vireo/tests/test_config.py -v
```

Expected: all pass.

### Step 5.4 — Commit

```bash
git commit -m "$(cat <<'EOF'
highlights: drop dead MMR selector module

The species-bucketed redesign replaces select_highlights with simple
Python-side bucketing. vireo/selection.py (used by encounter selection)
is unaffected.

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>
EOF
)"
```

---

## Task 6: Open the PR

### Step 6.1 — Push and open

```bash
git push -u origin highlights-page-redesign
gh pr create --base main --title "Redesign Highlights as a species-bucketed view" --body "$(cat <<'EOF'
## Summary

- New Highlights page: one row per species (accepted tag, or top non-rejected prediction at or above the confidence slider), with inline expand. Unidentified pinned below.
- New controls: Auto-ID confidence slider, per-row count, sort dropdown. Drops the old Count slider.
- Backend: \`get_highlights_candidates\` now surfaces \`predicted_species\`/\`predicted_confidence\`; \`/api/highlights\` returns a buckets-and-unidentified shape.
- Dead code removed: \`vireo/highlights.py\` (MMR selector) and its tests.

Design doc: \`docs/plans/2026-05-27-highlights-redesign-design.md\`.

## Test plan

- [ ] \`python -m pytest tests/test_workspaces.py vireo/tests/test_db.py vireo/tests/test_app.py vireo/tests/test_photos_api.py vireo/tests/test_edits_api.py vireo/tests/test_jobs_api.py vireo/tests/test_darktable_api.py vireo/tests/test_config.py -v\` passes locally.
- [ ] Browser walkthrough in the Hawaii workspace (results pasted in a comment).

🤖 Generated with [Claude Code](https://claude.com/claude-code)
EOF
)"
```

---

## Out of scope

These are explicitly **not** in this plan; leave for a follow-up if needed:

- Per-species "Save this row as collection" buttons (the design doc considered and deferred — Smart Collections already filter by species).
- Pagination of the API response. Hawaii's ~5k photos fit comfortably in one JSON payload; we'll revisit if anyone has a workspace big enough to feel it.
- Multi-species photos showing up in more than one bucket. We pick one species per photo today.
- Server-side sort. Sort is client-side; all metadata needed is already in the response.
