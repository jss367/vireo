# Detection Storage Redesign Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Move MegaDetector and classifier output from per-workspace to global caches keyed by deterministic inputs, and move confidence thresholds from write-time gates to read-time filters.

**Architecture:** Split "what the model produced" (global `detections` / `predictions`) from "what the user decided" (workspace-scoped `prediction_review`). Track model runs explicitly in `detector_runs` / `classifier_runs` so empty-scene photos don't get re-detected. Classifier identity becomes `(classifier_model, labels_fingerprint)` so two workspaces running BioClip with different lists stay distinct. Threshold becomes a SQL `WHERE` clause resolved from workspace-effective config on every read.

**Tech Stack:** Flask, SQLite, Python, pytest. No frontend framework.

**Design doc:** `docs/plans/2026-04-23-detection-storage-redesign-design.md`

---

## Overview of phases

| Phase | Focus | Tasks |
|-------|-------|-------|
| 0 | New schema + helpers (additive, no behavior change) | 1–7 |
| 1 | One-shot migration of legacy data | 8–13 |
| 2 | Rewire detector write path | 14–17 |
| 3 | Rewire classifier write path | 18–19 |
| 4 | Read-time threshold filters | 20–25 |
| 5 | UX polish + changelog | 26–27 |

Run `python -m pytest tests/test_workspaces.py vireo/tests/test_db.py vireo/tests/test_app.py vireo/tests/test_photos_api.py vireo/tests/test_edits_api.py vireo/tests/test_jobs_api.py vireo/tests/test_darktable_api.py vireo/tests/test_config.py -v` after each phase to make sure nothing else broke.

---

## Phase 0 — New schema and helpers

These tasks add tables and helpers alongside the existing ones. No existing behavior changes yet.

### Task 1: Add new tables to CREATE script

**Files:**
- Modify: `vireo/db.py` (the `executescript` block starting around line 80 — same block that creates `folders`, `photos`, `detections`, etc.)
- Test: `vireo/tests/test_db.py`

**Step 1: Write failing test**

Add to `vireo/tests/test_db.py`:

```python
def test_new_cache_tables_exist(tmp_path):
    """detector_runs, classifier_runs, labels_fingerprints, prediction_review are created."""
    from db import Database
    db = Database(str(tmp_path / "test.db"))
    tables = {r['name'] for r in db.conn.execute(
        "SELECT name FROM sqlite_master WHERE type='table'"
    ).fetchall()}
    assert 'detector_runs' in tables
    assert 'classifier_runs' in tables
    assert 'labels_fingerprints' in tables
    assert 'prediction_review' in tables
```

**Step 2: Run test to verify it fails**

```
python -m pytest vireo/tests/test_db.py::test_new_cache_tables_exist -v
```

Expected: FAIL — tables don't exist.

**Step 3: Add the CREATE TABLE statements**

Inside the `self.conn.executescript("""...""")` block in `vireo/db.py` (the one containing the existing `CREATE TABLE IF NOT EXISTS detections` around line 163), append:

```sql
CREATE TABLE IF NOT EXISTS detector_runs (
    photo_id        INTEGER NOT NULL REFERENCES photos(id) ON DELETE CASCADE,
    detector_model  TEXT NOT NULL,
    run_at          TEXT DEFAULT (datetime('now')),
    box_count       INTEGER NOT NULL DEFAULT 0,
    PRIMARY KEY (photo_id, detector_model)
);

CREATE TABLE IF NOT EXISTS classifier_runs (
    detection_id         INTEGER NOT NULL REFERENCES detections(id) ON DELETE CASCADE,
    classifier_model     TEXT NOT NULL,
    labels_fingerprint   TEXT NOT NULL,
    run_at               TEXT DEFAULT (datetime('now')),
    prediction_count     INTEGER NOT NULL DEFAULT 0,
    PRIMARY KEY (detection_id, classifier_model, labels_fingerprint)
);

CREATE TABLE IF NOT EXISTS labels_fingerprints (
    fingerprint    TEXT PRIMARY KEY,
    display_name   TEXT,
    sources_json   TEXT,
    label_count    INTEGER,
    created_at     TEXT DEFAULT (datetime('now'))
);

CREATE TABLE IF NOT EXISTS prediction_review (
    prediction_id  INTEGER NOT NULL REFERENCES predictions(id) ON DELETE CASCADE,
    workspace_id   INTEGER NOT NULL REFERENCES workspaces(id) ON DELETE CASCADE,
    status         TEXT NOT NULL DEFAULT 'pending',
    reviewed_at    TEXT,
    individual     TEXT,
    group_id       TEXT,
    vote_count     INTEGER,
    total_votes    INTEGER,
    PRIMARY KEY (prediction_id, workspace_id)
);
```

Also add indexes next to the existing `idx_detections_*` block further down in the method:

```sql
CREATE INDEX IF NOT EXISTS idx_prediction_review_workspace
  ON prediction_review(workspace_id);
CREATE INDEX IF NOT EXISTS idx_classifier_runs_detection
  ON classifier_runs(detection_id);
```

**Step 4: Run test to verify it passes**

```
python -m pytest vireo/tests/test_db.py::test_new_cache_tables_exist -v
```

Expected: PASS.

**Step 5: Commit**

```
git add vireo/db.py vireo/tests/test_db.py
git commit -m "db: add detector_runs, classifier_runs, labels_fingerprints, prediction_review tables"
```

---

### Task 2: Labels fingerprint module

**Files:**
- Create: `vireo/labels_fingerprint.py`
- Test: `vireo/tests/test_labels_fingerprint.py` (new)

**Step 1: Write failing test**

```python
# vireo/tests/test_labels_fingerprint.py
import os, sys
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

from labels_fingerprint import compute_fingerprint, TOL_SENTINEL, LEGACY_SENTINEL


def test_fingerprint_is_stable_under_ordering_and_duplicates():
    a = compute_fingerprint(["Bald Eagle", "American Robin", "Bald Eagle"])
    b = compute_fingerprint(["American Robin", "Bald Eagle"])
    assert a == b
    assert len(a) == 12  # sha256 hex prefix


def test_fingerprint_differs_on_different_sets():
    a = compute_fingerprint(["Bald Eagle", "American Robin"])
    b = compute_fingerprint(["Bald Eagle", "Steller's Jay"])
    assert a != b


def test_tol_sentinel_when_no_labels():
    assert compute_fingerprint(None) == TOL_SENTINEL
    assert compute_fingerprint([]) == TOL_SENTINEL


def test_sentinels_are_fixed_strings():
    assert TOL_SENTINEL == "tol"
    assert LEGACY_SENTINEL == "legacy"
```

**Step 2: Run test to verify it fails**

```
python -m pytest vireo/tests/test_labels_fingerprint.py -v
```

Expected: FAIL — module not found.

**Step 3: Implement**

```python
# vireo/labels_fingerprint.py
"""Content-addressable fingerprint for a classifier's label set.

The classifier's output is a pure function of (model, labels, input). We key
cached predictions by (classifier_model, labels_fingerprint) so two workspaces
running the same model with different regional lists stay disjoint rather than
conflicting or silently clobbering each other.
"""

import hashlib

TOL_SENTINEL = "tol"
LEGACY_SENTINEL = "legacy"


def compute_fingerprint(labels):
    """sha256 hex prefix of sorted, deduped labels. TOL_SENTINEL when empty."""
    if not labels:
        return TOL_SENTINEL
    canonical = "\n".join(sorted(set(labels))).encode("utf-8")
    return hashlib.sha256(canonical).hexdigest()[:12]
```

**Step 4: Run test to verify it passes**

```
python -m pytest vireo/tests/test_labels_fingerprint.py -v
```

Expected: PASS.

**Step 5: Commit**

```
git add vireo/labels_fingerprint.py vireo/tests/test_labels_fingerprint.py
git commit -m "labels: add content-addressable fingerprint for label sets"
```

---

### Task 3: `db.record_detector_run` / `db.get_detector_run_photo_ids`

**Files:**
- Modify: `vireo/db.py` (add methods near `save_detections` around line 3815)
- Test: `vireo/tests/test_db.py`

**Step 1: Write failing tests**

Add to `vireo/tests/test_db.py`:

```python
def test_record_detector_run_and_lookup(tmp_path):
    from db import Database
    db = Database(str(tmp_path / "test.db"))
    folder_id = db.add_folder("/tmp/p")
    db._active_workspace_id = db.create_workspace("WS")
    db.add_workspace_folder(db._active_workspace_id, folder_id)
    photo_id = db.add_photo(folder_id, "a.jpg")

    # Initially: no runs recorded
    assert db.get_detector_run_photo_ids("megadetector-v6") == set()

    db.record_detector_run(photo_id, "megadetector-v6", box_count=0)
    assert db.get_detector_run_photo_ids("megadetector-v6") == {photo_id}

    # Re-recording is idempotent / updates box_count
    db.record_detector_run(photo_id, "megadetector-v6", box_count=3)
    row = db.conn.execute(
        "SELECT box_count FROM detector_runs WHERE photo_id=? AND detector_model=?",
        (photo_id, "megadetector-v6"),
    ).fetchone()
    assert row["box_count"] == 3


def test_detector_run_is_not_workspace_scoped(tmp_path):
    from db import Database
    db = Database(str(tmp_path / "test.db"))
    folder_id = db.add_folder("/tmp/p")
    ws_a = db.create_workspace("A")
    ws_b = db.create_workspace("B")
    db.add_workspace_folder(ws_a, folder_id)
    db.add_workspace_folder(ws_b, folder_id)
    photo_id = db.add_photo(folder_id, "a.jpg")

    db._active_workspace_id = ws_a
    db.record_detector_run(photo_id, "megadetector-v6", box_count=2)

    db._active_workspace_id = ws_b
    assert photo_id in db.get_detector_run_photo_ids("megadetector-v6")
```

**Step 2: Run tests to verify they fail**

```
python -m pytest vireo/tests/test_db.py::test_record_detector_run_and_lookup vireo/tests/test_db.py::test_detector_run_is_not_workspace_scoped -v
```

Expected: FAIL — methods don't exist.

**Step 3: Implement**

Add methods near the existing `save_detections` / `get_detections` in `vireo/db.py`:

```python
def record_detector_run(self, photo_id, detector_model, box_count):
    """Record that `detector_model` was run on `photo_id`.

    Global across workspaces — the output is a pure function of (photo, model).
    """
    self.conn.execute(
        """INSERT INTO detector_runs (photo_id, detector_model, box_count)
           VALUES (?, ?, ?)
           ON CONFLICT(photo_id, detector_model)
           DO UPDATE SET box_count = excluded.box_count,
                         run_at = datetime('now')""",
        (photo_id, detector_model, box_count),
    )
    self.conn.commit()

def get_detector_run_photo_ids(self, detector_model):
    """Return the set of photo_ids where `detector_model` has run.

    Includes empty-scene photos (box_count=0) — which is the whole point:
    without this, we'd re-run the model forever on photos with no animals.
    """
    rows = self.conn.execute(
        "SELECT photo_id FROM detector_runs WHERE detector_model = ?",
        (detector_model,),
    ).fetchall()
    return {r["photo_id"] for r in rows}
```

**Step 4: Run tests to verify they pass**

```
python -m pytest vireo/tests/test_db.py::test_record_detector_run_and_lookup vireo/tests/test_db.py::test_detector_run_is_not_workspace_scoped -v
```

Expected: PASS.

**Step 5: Commit**

```
git add vireo/db.py vireo/tests/test_db.py
git commit -m "db: add detector_runs helpers (record + lookup)"
```

---

### Task 4: `db.record_classifier_run` / `db.get_classifier_run_keys`

**Files:**
- Modify: `vireo/db.py`
- Test: `vireo/tests/test_db.py`

**Step 1: Write failing tests**

```python
def test_record_classifier_run_and_lookup(tmp_path):
    from db import Database
    db = Database(str(tmp_path / "test.db"))
    folder_id = db.add_folder("/tmp/p")
    db._active_workspace_id = db.create_workspace("WS")
    db.add_workspace_folder(db._active_workspace_id, folder_id)
    photo_id = db.add_photo(folder_id, "a.jpg")
    # Need a detection row to reference:
    det_ids = db.save_detections(
        photo_id,
        [{"box": {"x": 0, "y": 0, "w": 1, "h": 1}, "confidence": 0.9, "category": "animal"}],
        detector_model="megadetector-v6",
    )
    det_id = det_ids[0]

    assert db.get_classifier_run_keys(det_id) == set()

    db.record_classifier_run(det_id, "bioclip-2", "abc123", prediction_count=5)
    assert db.get_classifier_run_keys(det_id) == {("bioclip-2", "abc123")}
```

**Step 2: Run tests to verify they fail**

```
python -m pytest vireo/tests/test_db.py::test_record_classifier_run_and_lookup -v
```

Expected: FAIL.

**Step 3: Implement**

Add near the detector-runs helpers in `vireo/db.py`:

```python
def record_classifier_run(self, detection_id, classifier_model,
                           labels_fingerprint, prediction_count):
    self.conn.execute(
        """INSERT INTO classifier_runs
             (detection_id, classifier_model, labels_fingerprint, prediction_count)
           VALUES (?, ?, ?, ?)
           ON CONFLICT(detection_id, classifier_model, labels_fingerprint)
           DO UPDATE SET prediction_count = excluded.prediction_count,
                         run_at = datetime('now')""",
        (detection_id, classifier_model, labels_fingerprint, prediction_count),
    )
    self.conn.commit()

def get_classifier_run_keys(self, detection_id):
    rows = self.conn.execute(
        """SELECT classifier_model, labels_fingerprint
           FROM classifier_runs
           WHERE detection_id = ?""",
        (detection_id,),
    ).fetchall()
    return {(r["classifier_model"], r["labels_fingerprint"]) for r in rows}
```

**Step 4: Run test to verify it passes**

```
python -m pytest vireo/tests/test_db.py::test_record_classifier_run_and_lookup -v
```

Expected: PASS.

**Step 5: Commit**

```
git add vireo/db.py vireo/tests/test_db.py
git commit -m "db: add classifier_runs helpers"
```

---

### Task 5: `db.upsert_labels_fingerprint` sidecar

**Files:**
- Modify: `vireo/db.py`
- Test: `vireo/tests/test_db.py`

**Step 1: Write failing test**

```python
def test_upsert_labels_fingerprint(tmp_path):
    from db import Database
    import json
    db = Database(str(tmp_path / "test.db"))
    db.upsert_labels_fingerprint(
        fingerprint="abc123",
        display_name="California birds",
        sources=["/labels/ca-birds.txt"],
        label_count=423,
    )
    row = db.conn.execute(
        "SELECT * FROM labels_fingerprints WHERE fingerprint=?", ("abc123",)
    ).fetchone()
    assert row["display_name"] == "California birds"
    assert json.loads(row["sources_json"]) == ["/labels/ca-birds.txt"]
    assert row["label_count"] == 423

    # Upsert is idempotent
    db.upsert_labels_fingerprint("abc123", "California birds (v2)",
                                  ["/labels/ca-birds-v2.txt"], 500)
    row = db.conn.execute(
        "SELECT display_name, label_count FROM labels_fingerprints WHERE fingerprint=?",
        ("abc123",),
    ).fetchone()
    assert row["display_name"] == "California birds (v2)"
    assert row["label_count"] == 500
```

**Step 2: Run test to verify it fails**

Expected: FAIL.

**Step 3: Implement**

```python
def upsert_labels_fingerprint(self, fingerprint, display_name, sources, label_count):
    import json
    self.conn.execute(
        """INSERT INTO labels_fingerprints
             (fingerprint, display_name, sources_json, label_count)
           VALUES (?, ?, ?, ?)
           ON CONFLICT(fingerprint)
           DO UPDATE SET display_name = excluded.display_name,
                         sources_json = excluded.sources_json,
                         label_count  = excluded.label_count""",
        (fingerprint, display_name, json.dumps(sources or []), label_count),
    )
    self.conn.commit()
```

**Step 4: Run test to verify it passes**

Expected: PASS.

**Step 5: Commit**

```
git add vireo/db.py vireo/tests/test_db.py
git commit -m "db: add labels_fingerprints upsert helper"
```

---

### Task 6: `db.get_review_status` / `db.set_review_status`

Absence of a row means `'pending'` — implement that in the reader.

**Files:**
- Modify: `vireo/db.py`
- Test: `vireo/tests/test_db.py`

**Step 1: Write failing tests**

```python
def test_review_status_absence_is_pending(tmp_path):
    from db import Database
    db = Database(str(tmp_path / "test.db"))
    folder_id = db.add_folder("/tmp/p")
    ws = db.create_workspace("WS")
    db._active_workspace_id = ws
    db.add_workspace_folder(ws, folder_id)
    photo_id = db.add_photo(folder_id, "a.jpg")
    det_ids = db.save_detections(
        photo_id,
        [{"box": {"x": 0, "y": 0, "w": 1, "h": 1}, "confidence": 0.9, "category": "animal"}],
        detector_model="megadetector-v6",
    )
    pred_id = db.conn.execute(
        """INSERT INTO predictions (detection_id, classifier_model, labels_fingerprint,
                                    species, confidence)
           VALUES (?, 'bioclip-2', 'abc', 'Robin', 0.8)""",
        (det_ids[0],),
    ).lastrowid
    db.conn.commit()

    # No row in prediction_review yet → pending
    assert db.get_review_status(pred_id, ws) == "pending"

    db.set_review_status(pred_id, ws, status="approved")
    assert db.get_review_status(pred_id, ws) == "approved"

    db.set_review_status(pred_id, ws, status="rejected")
    assert db.get_review_status(pred_id, ws) == "rejected"
```

**Step 2: Run test to verify it fails**

Expected: FAIL.

**Step 3: Implement**

```python
def get_review_status(self, prediction_id, workspace_id):
    row = self.conn.execute(
        """SELECT status FROM prediction_review
           WHERE prediction_id = ? AND workspace_id = ?""",
        (prediction_id, workspace_id),
    ).fetchone()
    return row["status"] if row else "pending"

def set_review_status(self, prediction_id, workspace_id, status,
                       individual=None, group_id=None):
    self.conn.execute(
        """INSERT INTO prediction_review
             (prediction_id, workspace_id, status, reviewed_at, individual, group_id)
           VALUES (?, ?, ?, datetime('now'), ?, ?)
           ON CONFLICT(prediction_id, workspace_id)
           DO UPDATE SET status      = excluded.status,
                         reviewed_at = excluded.reviewed_at,
                         individual  = COALESCE(excluded.individual, individual),
                         group_id    = COALESCE(excluded.group_id,   group_id)""",
        (prediction_id, workspace_id, status, individual, group_id),
    )
    self.conn.commit()
```

**Step 4: Run test to verify it passes**

Expected: PASS.

**Step 5: Commit**

```
git add vireo/db.py vireo/tests/test_db.py
git commit -m "db: add prediction_review get/set with absence-means-pending semantics"
```

---

### Task 7: Phase 0 regression run

**Step 1:** Run the full suite — nothing should be broken since everything is additive.

```
python -m pytest tests/test_workspaces.py vireo/tests/test_db.py vireo/tests/test_app.py vireo/tests/test_photos_api.py vireo/tests/test_edits_api.py vireo/tests/test_jobs_api.py vireo/tests/test_config.py -v
```

Expected: all pre-existing tests still pass.

**Step 2: No commit needed.** If regressions appear, investigate before moving to Phase 1.

---

## Phase 1 — Migration of legacy data

These tasks convert existing workspace-scoped `detections` and `predictions` tables into the new global shape. Structured so each task is individually committable and the DB is usable at every step in between.

### Task 8: Migration — backfill `detector_runs`

**Files:**
- Modify: `vireo/db.py` — the migration block that runs during `__init__` (after the new CREATE TABLE statements run, so `detector_runs` exists).
- Test: `vireo/tests/test_db.py`

**Step 1: Write failing test**

```python
def test_migration_backfills_detector_runs(tmp_path):
    """Legacy detections become detector_runs rows on upgrade."""
    import sqlite3
    db_path = str(tmp_path / "legacy.db")
    conn = sqlite3.connect(db_path)
    conn.executescript("""
        CREATE TABLE folders (id INTEGER PRIMARY KEY, path TEXT);
        CREATE TABLE photos (id INTEGER PRIMARY KEY, folder_id INTEGER,
                             filename TEXT, UNIQUE(folder_id, filename));
        CREATE TABLE workspaces (id INTEGER PRIMARY KEY, name TEXT UNIQUE);
        CREATE TABLE detections (
            id INTEGER PRIMARY KEY,
            photo_id INTEGER,
            workspace_id INTEGER,
            box_x REAL, box_y REAL, box_w REAL, box_h REAL,
            detector_confidence REAL,
            category TEXT,
            detector_model TEXT,
            created_at TEXT
        );
        INSERT INTO folders (id, path) VALUES (1, '/p');
        INSERT INTO photos (id, folder_id, filename) VALUES (10, 1, 'a.jpg');
        INSERT INTO workspaces (id, name) VALUES (1, 'Default');
        INSERT INTO detections (photo_id, workspace_id, box_x, box_y, box_w, box_h,
                                detector_confidence, category, detector_model, created_at)
            VALUES (10, 1, 0, 0, 1, 1, 0.5, 'animal', 'megadetector-v6', '2026-01-01T00:00:00');
    """)
    conn.commit()
    conn.close()

    # Open through Database → migrations run
    from db import Database
    db = Database(db_path)
    run = db.conn.execute(
        "SELECT box_count FROM detector_runs WHERE photo_id=10 AND detector_model='megadetector-v6'"
    ).fetchone()
    assert run is not None
    assert run["box_count"] == 1
```

**Step 2: Run test to verify it fails**

Expected: FAIL — migration doesn't exist yet.

**Step 3: Implement**

In `vireo/db.py`, find the section where existing schema migrations run (after `executescript`, same place the multi-animal migration lives around line 575). Add:

```python
# detector_runs backfill (detection-storage redesign): derive one row per
# distinct (photo_id, detector_model) from existing detections so downstream
# skip checks don't re-run MegaDetector over photos it has already seen.
# Idempotent — only inserts rows whose (photo_id, detector_model) isn't
# already present.
existing_runs = self.conn.execute(
    "SELECT COUNT(*) AS n FROM detector_runs"
).fetchone()["n"]
legacy_detection_count = self.conn.execute(
    "SELECT COUNT(*) AS n FROM detections"
).fetchone()["n"]
if existing_runs == 0 and legacy_detection_count > 0:
    self.conn.execute(
        """INSERT OR IGNORE INTO detector_runs
             (photo_id, detector_model, box_count, run_at)
           SELECT photo_id, COALESCE(detector_model, 'megadetector-v6'),
                  COUNT(*), MIN(created_at)
           FROM detections
           GROUP BY photo_id, COALESCE(detector_model, 'megadetector-v6')"""
    )
    self.conn.commit()
```

**Step 4: Run test to verify it passes**

Expected: PASS.

**Step 5: Commit**

```
git add vireo/db.py vireo/tests/test_db.py
git commit -m "db: migrate existing detections into detector_runs on upgrade"
```

---

### Task 9: Migration — dedupe detections & drop `workspace_id`

This is the highest-risk task. Follow the existing pattern used by the multi-animal migration (`CREATE detections_new`, `INSERT SELECT DISTINCT`, repoint predictions, `DROP`, `RENAME`).

**Files:**
- Modify: `vireo/db.py`
- Test: `vireo/tests/test_db.py`

**Step 1: Write failing test**

```python
def test_migration_dedupes_detections_and_repoints_predictions(tmp_path):
    """Two workspaces with identical detection rows collapse to one; predictions follow."""
    import sqlite3
    db_path = str(tmp_path / "legacy.db")
    conn = sqlite3.connect(db_path)
    conn.executescript("""
        CREATE TABLE folders (id INTEGER PRIMARY KEY, path TEXT);
        CREATE TABLE photos (id INTEGER PRIMARY KEY, folder_id INTEGER,
                             filename TEXT, UNIQUE(folder_id, filename));
        CREATE TABLE workspaces (id INTEGER PRIMARY KEY, name TEXT UNIQUE);
        CREATE TABLE detections (
            id INTEGER PRIMARY KEY, photo_id INTEGER, workspace_id INTEGER,
            box_x REAL, box_y REAL, box_w REAL, box_h REAL,
            detector_confidence REAL, category TEXT, detector_model TEXT,
            created_at TEXT
        );
        CREATE TABLE predictions (
            id INTEGER PRIMARY KEY, detection_id INTEGER, species TEXT,
            confidence REAL, model TEXT, status TEXT DEFAULT 'pending',
            individual TEXT, group_id TEXT, created_at TEXT
        );
        INSERT INTO folders VALUES (1, '/p');
        INSERT INTO photos  VALUES (10, 1, 'a.jpg');
        INSERT INTO workspaces VALUES (1, 'A'), (2, 'B');
        -- Same box, same photo, two workspaces:
        INSERT INTO detections (id, photo_id, workspace_id, box_x, box_y, box_w, box_h,
                                detector_confidence, category, detector_model, created_at)
          VALUES (100, 10, 1, 0.1, 0.1, 0.5, 0.5, 0.9, 'animal', 'megadetector-v6', 't1'),
                 (200, 10, 2, 0.1, 0.1, 0.5, 0.5, 0.9, 'animal', 'megadetector-v6', 't2');
        INSERT INTO predictions (id, detection_id, species, model, status) VALUES
            (1000, 100, 'Robin', 'bioclip-2', 'approved'),
            (2000, 200, 'Robin', 'bioclip-2', 'pending');
    """)
    conn.commit()
    conn.close()

    from db import Database
    db = Database(db_path)
    # Exactly one detection row for (photo=10, model=megadetector-v6)
    rows = db.conn.execute(
        "SELECT id FROM detections WHERE photo_id=10 AND detector_model='megadetector-v6'"
    ).fetchall()
    assert len(rows) == 1
    canonical_id = rows[0]["id"]
    # Both predictions now point at the canonical detection id
    pred_rows = db.conn.execute(
        "SELECT id, detection_id FROM predictions ORDER BY id"
    ).fetchall()
    assert {r["detection_id"] for r in pred_rows} == {canonical_id}
    # detections table no longer has workspace_id column
    cols = {r[1] for r in db.conn.execute("PRAGMA table_info(detections)").fetchall()}
    assert "workspace_id" not in cols
```

**Step 2: Run test to verify it fails**

Expected: FAIL.

**Step 3: Implement**

Add after the Task 8 backfill in `vireo/db.py`:

```python
# Global-detections migration: drop workspace_id from detections and dedupe
# identical boxes that were duplicated across workspaces. Re-point predictions
# at the canonical detection id.
#
# Follows the existing "create new, copy, drop old, rename" pattern used by
# the multi-animal migration above. Gated on the `detections` table still
# having a workspace_id column.
det_cols = {r[1] for r in self.conn.execute(
    "PRAGMA table_info(detections)"
).fetchall()}
if "workspace_id" in det_cols:
    # Pick the lowest id per group as canonical.
    self.conn.execute("""
        CREATE TEMP TABLE detection_canonical AS
        SELECT MIN(id) AS canonical_id, photo_id,
               COALESCE(detector_model, 'megadetector-v6') AS detector_model,
               box_x, box_y, box_w, box_h
        FROM detections
        GROUP BY photo_id, COALESCE(detector_model, 'megadetector-v6'),
                 box_x, box_y, box_w, box_h
    """)
    # Re-point predictions that reference a non-canonical duplicate.
    self.conn.execute("""
        UPDATE predictions
        SET detection_id = (
            SELECT dc.canonical_id
            FROM detections d
            JOIN detection_canonical dc
              ON dc.photo_id       = d.photo_id
             AND dc.detector_model = COALESCE(d.detector_model, 'megadetector-v6')
             AND dc.box_x = d.box_x AND dc.box_y = d.box_y
             AND dc.box_w = d.box_w AND dc.box_h = d.box_h
            WHERE d.id = predictions.detection_id
        )
        WHERE detection_id IN (
            SELECT d.id
            FROM detections d
            JOIN detection_canonical dc
              ON dc.photo_id       = d.photo_id
             AND dc.detector_model = COALESCE(d.detector_model, 'megadetector-v6')
             AND dc.box_x = d.box_x AND dc.box_y = d.box_y
             AND dc.box_w = d.box_w AND dc.box_h = d.box_h
            WHERE d.id <> dc.canonical_id
        )
    """)
    # Create new table without workspace_id
    self.conn.execute("""
        CREATE TABLE detections_new (
            id                   INTEGER PRIMARY KEY,
            photo_id             INTEGER NOT NULL REFERENCES photos(id) ON DELETE CASCADE,
            detector_model       TEXT NOT NULL DEFAULT 'megadetector-v6',
            box_x REAL, box_y REAL, box_w REAL, box_h REAL,
            detector_confidence  REAL,
            category             TEXT,
            created_at           TEXT DEFAULT (datetime('now'))
        )
    """)
    self.conn.execute("""
        INSERT INTO detections_new (id, photo_id, detector_model,
                                    box_x, box_y, box_w, box_h,
                                    detector_confidence, category, created_at)
        SELECT d.id, d.photo_id,
               COALESCE(d.detector_model, 'megadetector-v6'),
               d.box_x, d.box_y, d.box_w, d.box_h,
               d.detector_confidence, d.category, d.created_at
        FROM detections d
        JOIN detection_canonical dc ON dc.canonical_id = d.id
    """)
    self.conn.execute("DROP TABLE detections")
    self.conn.execute("ALTER TABLE detections_new RENAME TO detections")
    self.conn.execute("DROP TABLE detection_canonical")
    # Recreate the indexes (the CREATE INDEX IF NOT EXISTS at the bottom of
    # __init__ will no-op if they already exist, but indexes tied to the
    # dropped table are gone).
    self.conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_detections_photo ON detections(photo_id)"
    )
    self.conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_detections_photo_model "
        "ON detections(photo_id, detector_model)"
    )
    self.conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_detections_conf "
        "ON detections(photo_id, detector_confidence)"
    )
    self.conn.commit()
```

**Step 4: Run test to verify it passes**

```
python -m pytest vireo/tests/test_db.py::test_migration_dedupes_detections_and_repoints_predictions -v
```

Expected: PASS. Also run the previous migration test (Task 8) to make sure it still passes — it should, since dedupe is a no-op when everything is unique.

**Step 5: Commit**

```
git add vireo/db.py vireo/tests/test_db.py
git commit -m "db: drop detections.workspace_id, dedupe across workspaces, repoint predictions"
```

---

### Task 10: Migration — add `labels_fingerprint` column to predictions

Tiny additive migration.

**Files:**
- Modify: `vireo/db.py` (migration block; also the CREATE script so fresh DBs get it too)
- Test: `vireo/tests/test_db.py`

**Step 1: Write failing test**

```python
def test_predictions_has_labels_fingerprint(tmp_path):
    from db import Database
    db = Database(str(tmp_path / "test.db"))
    cols = {r[1] for r in db.conn.execute(
        "PRAGMA table_info(predictions)"
    ).fetchall()}
    assert "labels_fingerprint" in cols


def test_migration_sets_labels_fingerprint_legacy(tmp_path):
    import sqlite3
    db_path = str(tmp_path / "legacy.db")
    conn = sqlite3.connect(db_path)
    conn.executescript("""
        CREATE TABLE folders (id INTEGER PRIMARY KEY, path TEXT);
        CREATE TABLE photos (id INTEGER PRIMARY KEY, folder_id INTEGER,
                             filename TEXT, UNIQUE(folder_id, filename));
        CREATE TABLE workspaces (id INTEGER PRIMARY KEY, name TEXT UNIQUE);
        CREATE TABLE detections (
            id INTEGER PRIMARY KEY, photo_id INTEGER, workspace_id INTEGER,
            box_x REAL, box_y REAL, box_w REAL, box_h REAL,
            detector_confidence REAL, category TEXT, detector_model TEXT,
            created_at TEXT
        );
        CREATE TABLE predictions (
            id INTEGER PRIMARY KEY, detection_id INTEGER, species TEXT,
            confidence REAL, model TEXT, status TEXT DEFAULT 'pending',
            individual TEXT, group_id TEXT, created_at TEXT
        );
        INSERT INTO folders VALUES (1, '/p');
        INSERT INTO photos VALUES (10, 1, 'a.jpg');
        INSERT INTO workspaces VALUES (1, 'A');
        INSERT INTO detections (id, photo_id, workspace_id, box_x, box_y, box_w, box_h,
                                detector_confidence, category, detector_model, created_at)
            VALUES (100, 10, 1, 0, 0, 1, 1, 0.9, 'animal', 'megadetector-v6', 't1');
        INSERT INTO predictions (id, detection_id, species, model) VALUES
            (1, 100, 'Robin', 'bioclip-2');
    """)
    conn.commit()
    conn.close()

    from db import Database
    db = Database(db_path)
    row = db.conn.execute(
        "SELECT labels_fingerprint FROM predictions WHERE id=1"
    ).fetchone()
    assert row["labels_fingerprint"] == "legacy"
```

**Step 2: Run tests to verify they fail**

Expected: FAIL.

**Step 3: Implement**

Two changes to `vireo/db.py`:

1. In the main `CREATE TABLE predictions` block near line 177, add `labels_fingerprint TEXT NOT NULL DEFAULT 'legacy'` as a column. (Don't add the new UNIQUE constraint yet — Task 12 handles that.)

2. In the migration block (after Task 9's migration), add:

```python
# Add labels_fingerprint column if missing. Existing rows get 'legacy'.
pred_cols = {r[1] for r in self.conn.execute(
    "PRAGMA table_info(predictions)"
).fetchall()}
if "labels_fingerprint" not in pred_cols:
    self.conn.execute(
        "ALTER TABLE predictions ADD COLUMN labels_fingerprint TEXT "
        "NOT NULL DEFAULT 'legacy'"
    )
    self.conn.commit()
```

**Step 4: Run tests to verify they pass**

Expected: PASS.

**Step 5: Commit**

```
git add vireo/db.py vireo/tests/test_db.py
git commit -m "db: add labels_fingerprint to predictions, default 'legacy' for migrated rows"
```

---

### Task 11: Migration — backfill `prediction_review` from legacy columns

**Files:**
- Modify: `vireo/db.py`
- Test: `vireo/tests/test_db.py`

**Step 1: Write failing test**

```python
def test_migration_backfills_prediction_review(tmp_path):
    """Approved/rejected predictions get prediction_review rows in the right workspace."""
    import sqlite3
    db_path = str(tmp_path / "legacy.db")
    conn = sqlite3.connect(db_path)
    conn.executescript("""
        CREATE TABLE folders (id INTEGER PRIMARY KEY, path TEXT);
        CREATE TABLE photos (id INTEGER PRIMARY KEY, folder_id INTEGER,
                             filename TEXT, UNIQUE(folder_id, filename));
        CREATE TABLE workspaces (id INTEGER PRIMARY KEY, name TEXT UNIQUE);
        CREATE TABLE detections (
            id INTEGER PRIMARY KEY, photo_id INTEGER, workspace_id INTEGER,
            box_x REAL, box_y REAL, box_w REAL, box_h REAL,
            detector_confidence REAL, category TEXT, detector_model TEXT,
            created_at TEXT
        );
        CREATE TABLE predictions (
            id INTEGER PRIMARY KEY, detection_id INTEGER, species TEXT,
            confidence REAL, model TEXT,
            status TEXT DEFAULT 'pending', reviewed_at TEXT,
            individual TEXT, group_id TEXT,
            vote_count INTEGER, total_votes INTEGER,
            created_at TEXT
        );
        INSERT INTO folders VALUES (1, '/p');
        INSERT INTO photos VALUES (10, 1, 'a.jpg');
        INSERT INTO workspaces VALUES (1, 'A'), (2, 'B');
        INSERT INTO detections (id, photo_id, workspace_id, box_x, box_y, box_w, box_h,
                                detector_confidence, category, detector_model, created_at)
            VALUES (100, 10, 1, 0, 0, 1, 1, 0.9, 'animal', 'megadetector-v6', 't1'),
                   (200, 10, 2, 0, 0, 1, 1, 0.9, 'animal', 'megadetector-v6', 't2');
        INSERT INTO predictions (id, detection_id, species, model, status, individual)
            VALUES (1, 100, 'Robin', 'bioclip-2', 'approved', 'Ruby'),
                   (2, 200, 'Robin', 'bioclip-2', 'rejected', NULL);
    """)
    conn.commit()
    conn.close()

    from db import Database
    db = Database(db_path)
    rows = db.conn.execute(
        "SELECT prediction_id, workspace_id, status, individual "
        "FROM prediction_review ORDER BY workspace_id"
    ).fetchall()
    # After Task 9 dedupes detections, both predictions point to the canonical
    # detection, so we should have two review rows — one per workspace.
    assert len(rows) == 2
    ws_statuses = {r["workspace_id"]: (r["status"], r["individual"]) for r in rows}
    assert ws_statuses[1] == ("approved", "Ruby")
    assert ws_statuses[2] == ("rejected", None)
```

**Step 2: Run test to verify it fails**

Expected: FAIL.

**Step 3: Implement**

Add after the Task 10 migration in `vireo/db.py`:

```python
# Backfill prediction_review from legacy per-prediction review columns.
# Only runs once (when predictions still has a `status` column).
pred_cols = {r[1] for r in self.conn.execute(
    "PRAGMA table_info(predictions)"
).fetchall()}
if "status" in pred_cols:
    # Derive workspace_id from the owning detection's pre-migration
    # workspace_id... which we've already dropped. Use the post-dedupe
    # predictions table plus the detection_canonical temp — except we've
    # dropped that too. Cleanest path: rebuild a (prediction_id -> workspace_id)
    # map by joining through the ORIGINAL detection IDs captured *before*
    # we dropped workspace_id.
    #
    # Because Tasks 8–10 already ran above, we can no longer recover the
    # workspace_id from detections. Instead: do this backfill BEFORE Task 9's
    # drop. Move the snippet earlier. See Note below.
    pass
```

Because the prediction_review backfill needs `detections.workspace_id`, the ordering inside the single migration function must be:

1. Task 8 backfill — detector_runs (still has workspace_id).
2. **Task 11 backfill** — prediction_review (still has workspace_id).
3. Task 9 — dedupe + drop workspace_id.
4. Task 10 — labels_fingerprint column.
5. Task 12 — drop review columns from predictions.

Refactor the migration code in `vireo/db.py` to execute in that order. The Task 11 body becomes:

```python
# Backfill prediction_review from legacy per-prediction review columns.
# Must run before the detections.workspace_id drop so we can route each
# prediction to the correct workspace.
pred_cols = {r[1] for r in self.conn.execute(
    "PRAGMA table_info(predictions)"
).fetchall()}
review_exists = self.conn.execute(
    "SELECT COUNT(*) AS n FROM prediction_review"
).fetchone()["n"]
if "status" in pred_cols and review_exists == 0:
    self.conn.execute("""
        INSERT OR IGNORE INTO prediction_review
            (prediction_id, workspace_id, status, reviewed_at,
             individual, group_id, vote_count, total_votes)
        SELECT p.id, d.workspace_id,
               COALESCE(p.status, 'pending'),
               p.reviewed_at, p.individual, p.group_id,
               p.vote_count, p.total_votes
        FROM predictions p
        JOIN detections d ON d.id = p.detection_id
        WHERE d.workspace_id IS NOT NULL
          AND COALESCE(p.status, 'pending') <> 'pending'
    """)
    self.conn.commit()
```

Restrict the backfill to non-pending rows — `'pending'` is the default and is implied by row absence, so we don't need those rows.

**Step 4: Run test to verify it passes**

Expected: PASS.

**Step 5: Commit**

```
git add vireo/db.py vireo/tests/test_db.py
git commit -m "db: backfill prediction_review from legacy predictions columns"
```

---

### Task 12: Migration — drop legacy columns from `predictions`, add new UNIQUE

**Files:**
- Modify: `vireo/db.py` (both the CREATE script and the migration block)
- Test: `vireo/tests/test_db.py`

**Step 1: Write failing test**

```python
def test_predictions_has_new_unique_and_no_legacy_columns(tmp_path):
    from db import Database
    db = Database(str(tmp_path / "test.db"))
    cols = {r[1] for r in db.conn.execute(
        "PRAGMA table_info(predictions)"
    ).fetchall()}
    # Legacy review/workspace columns are gone
    for legacy in ("status", "reviewed_at", "individual", "group_id",
                   "vote_count", "total_votes", "workspace_id"):
        assert legacy not in cols, f"legacy column {legacy} still present"
    # New unique constraint on (detection_id, classifier_model, labels_fingerprint, species)
    indexes = db.conn.execute(
        "SELECT name, sql FROM sqlite_master WHERE type='index' AND tbl_name='predictions'"
    ).fetchall()
    assert any(
        "labels_fingerprint" in (idx["sql"] or "") and "species" in (idx["sql"] or "")
        for idx in indexes
    )
```

**Step 2: Run test to verify it fails**

Expected: FAIL.

**Step 3: Implement**

Update the main `CREATE TABLE predictions` to:

```sql
CREATE TABLE IF NOT EXISTS predictions (
    id                   INTEGER PRIMARY KEY,
    detection_id         INTEGER NOT NULL REFERENCES detections(id) ON DELETE CASCADE,
    classifier_model     TEXT NOT NULL,
    labels_fingerprint   TEXT NOT NULL DEFAULT 'legacy',
    species              TEXT,
    confidence           REAL,
    category             TEXT,
    scientific_name      TEXT,
    taxonomy_kingdom     TEXT,
    taxonomy_phylum      TEXT,
    taxonomy_class       TEXT,
    taxonomy_order       TEXT,
    taxonomy_family      TEXT,
    taxonomy_genus       TEXT,
    created_at           TEXT DEFAULT (datetime('now')),
    UNIQUE(detection_id, classifier_model, labels_fingerprint, species)
);
```

(Rename `model` → `classifier_model`; drop review columns; add `labels_fingerprint` with default; add new UNIQUE.)

Migration block (after Task 11 backfill):

```python
# Drop review + legacy model columns, rename `model` -> `classifier_model`,
# apply new UNIQUE key. Uses create-copy-drop-rename because SQLite
# can't drop columns + add constraints atomically.
pred_cols = {r[1] for r in self.conn.execute(
    "PRAGMA table_info(predictions)"
).fetchall()}
needs_pred_rewrite = "status" in pred_cols or "model" in pred_cols
if needs_pred_rewrite:
    self.conn.execute("""
        CREATE TABLE predictions_new (
            id                   INTEGER PRIMARY KEY,
            detection_id         INTEGER NOT NULL REFERENCES detections(id) ON DELETE CASCADE,
            classifier_model     TEXT NOT NULL,
            labels_fingerprint   TEXT NOT NULL DEFAULT 'legacy',
            species              TEXT,
            confidence           REAL,
            category             TEXT,
            scientific_name      TEXT,
            taxonomy_kingdom     TEXT,
            taxonomy_phylum      TEXT,
            taxonomy_class       TEXT,
            taxonomy_order       TEXT,
            taxonomy_family      TEXT,
            taxonomy_genus       TEXT,
            created_at           TEXT DEFAULT (datetime('now')),
            UNIQUE(detection_id, classifier_model, labels_fingerprint, species)
        )
    """)
    self.conn.execute("""
        INSERT OR IGNORE INTO predictions_new
            (id, detection_id, classifier_model, labels_fingerprint,
             species, confidence, category, scientific_name,
             taxonomy_kingdom, taxonomy_phylum, taxonomy_class,
             taxonomy_order, taxonomy_family, taxonomy_genus, created_at)
        SELECT id, detection_id,
               COALESCE(model, 'unknown'),
               COALESCE(labels_fingerprint, 'legacy'),
               species, confidence, category, scientific_name,
               taxonomy_kingdom, taxonomy_phylum, taxonomy_class,
               taxonomy_order, taxonomy_family, taxonomy_genus, created_at
        FROM predictions
    """)
    self.conn.execute("DROP TABLE predictions")
    self.conn.execute("ALTER TABLE predictions_new RENAME TO predictions")
    self.conn.commit()
```

**Step 4: Run test to verify it passes**

Expected: PASS.

**Step 5: Commit**

```
git add vireo/db.py vireo/tests/test_db.py
git commit -m "db: drop predictions.status/individual/group_id/model in favor of classifier_model + prediction_review"
```

---

### Task 13: End-to-end migration regression test

**Files:**
- Create: `vireo/tests/test_migration_detection_storage.py`

**Step 1: Write the test**

A single fixture that captures a realistic pre-migration DB (multi-workspace, overlapping detections, mixed review statuses) and asserts the post-migration shape is coherent.

```python
# vireo/tests/test_migration_detection_storage.py
import os, sys, sqlite3
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))


def _build_legacy_db(path):
    conn = sqlite3.connect(path)
    conn.executescript("""
        CREATE TABLE folders (id INTEGER PRIMARY KEY, path TEXT);
        CREATE TABLE photos (id INTEGER PRIMARY KEY, folder_id INTEGER,
                             filename TEXT, UNIQUE(folder_id, filename));
        CREATE TABLE workspaces (id INTEGER PRIMARY KEY, name TEXT UNIQUE);
        CREATE TABLE workspace_folders (
            workspace_id INTEGER, folder_id INTEGER,
            PRIMARY KEY (workspace_id, folder_id)
        );
        CREATE TABLE detections (
            id INTEGER PRIMARY KEY, photo_id INTEGER, workspace_id INTEGER,
            box_x REAL, box_y REAL, box_w REAL, box_h REAL,
            detector_confidence REAL, category TEXT, detector_model TEXT,
            created_at TEXT
        );
        CREATE TABLE predictions (
            id INTEGER PRIMARY KEY, detection_id INTEGER, species TEXT,
            confidence REAL, model TEXT,
            status TEXT DEFAULT 'pending', reviewed_at TEXT,
            individual TEXT, group_id TEXT,
            vote_count INTEGER, total_votes INTEGER,
            created_at TEXT
        );
        INSERT INTO folders VALUES (1, '/p');
        INSERT INTO photos VALUES (10, 1, 'a.jpg'), (11, 1, 'b.jpg');
        INSERT INTO workspaces VALUES (1, 'A'), (2, 'B');
        INSERT INTO workspace_folders VALUES (1, 1), (2, 1);
        -- photo 10 detected in both workspaces (same box -> dedupes)
        INSERT INTO detections (id, photo_id, workspace_id, box_x, box_y, box_w, box_h,
                                detector_confidence, category, detector_model, created_at)
          VALUES (100, 10, 1, 0.1, 0.1, 0.4, 0.4, 0.92, 'animal', 'megadetector-v6', 't1'),
                 (200, 10, 2, 0.1, 0.1, 0.4, 0.4, 0.92, 'animal', 'megadetector-v6', 't1'),
                 -- photo 11 only in workspace A, different box
                 (300, 11, 1, 0.2, 0.2, 0.5, 0.5, 0.71, 'animal', 'megadetector-v6', 't1');
        INSERT INTO predictions (id, detection_id, species, model,
                                 status, individual, group_id)
          VALUES (1, 100, 'Robin',  'bioclip-2', 'approved', 'Ruby', 'pair-01'),
                 (2, 200, 'Robin',  'bioclip-2', 'pending',  NULL,   NULL),
                 (3, 300, 'Sparrow','bioclip-2', 'rejected', NULL,   NULL);
    """)
    conn.commit()
    conn.close()


def test_full_migration(tmp_path):
    db_path = str(tmp_path / "legacy.db")
    _build_legacy_db(db_path)

    from db import Database
    db = Database(db_path)

    # detections: one row per unique box; workspace_id column gone
    det_cols = {r[1] for r in db.conn.execute("PRAGMA table_info(detections)").fetchall()}
    assert "workspace_id" not in det_cols
    photo10_rows = db.conn.execute(
        "SELECT id FROM detections WHERE photo_id=10"
    ).fetchall()
    assert len(photo10_rows) == 1
    canonical_10 = photo10_rows[0]["id"]

    # predictions re-pointed to canonical detection; legacy columns gone
    pred_cols = {r[1] for r in db.conn.execute("PRAGMA table_info(predictions)").fetchall()}
    for legacy in ("status", "individual", "group_id", "reviewed_at",
                   "vote_count", "total_votes", "model"):
        assert legacy not in pred_cols, f"legacy column {legacy} still present"
    photo10_preds = db.conn.execute(
        "SELECT id, detection_id FROM predictions "
        "WHERE detection_id = ? ORDER BY id",
        (canonical_10,),
    ).fetchall()
    # Two predictions (1, 2) both re-point to canonical_10
    assert {p["id"] for p in photo10_preds} == {1, 2}

    # review state landed in prediction_review
    reviews = db.conn.execute(
        "SELECT prediction_id, workspace_id, status, individual "
        "FROM prediction_review ORDER BY prediction_id, workspace_id"
    ).fetchall()
    review_map = {(r["prediction_id"], r["workspace_id"]):
                  (r["status"], r["individual"]) for r in reviews}
    # pred 1 was approved in ws 1, with individual "Ruby"
    assert review_map[(1, 1)] == ("approved", "Ruby")
    # pred 2 was pending in ws 2 → absence (not in review_map)
    assert (2, 2) not in review_map
    # pred 3 rejected in ws 1
    assert review_map[(3, 1)] == ("rejected", None)

    # detector_runs backfilled for every (photo, model)
    run_keys = {(r["photo_id"], r["detector_model"]) for r in db.conn.execute(
        "SELECT photo_id, detector_model FROM detector_runs"
    ).fetchall()}
    assert (10, "megadetector-v6") in run_keys
    assert (11, "megadetector-v6") in run_keys
```

**Step 2: Run test — it should already pass** after Tasks 8–12.

```
python -m pytest vireo/tests/test_migration_detection_storage.py -v
```

Expected: PASS. If it fails, debug before moving on.

**Step 3: Commit**

```
git add vireo/tests/test_migration_detection_storage.py
git commit -m "test: end-to-end migration regression for detection storage redesign"
```

---

## Phase 2 — Detector write path

### Task 14: Detector hard floor + simplified signature

**Files:**
- Modify: `vireo/detector.py`
- Test: `vireo/tests/test_detector.py`

**Step 1: Write failing test**

Add to `vireo/tests/test_detector.py`:

```python
def test_detect_animals_uses_hard_floor(monkeypatch):
    """detect_animals no longer takes a confidence_threshold param; it uses 0.01 floor."""
    from detector import RAW_CONF_FLOOR
    assert RAW_CONF_FLOOR == 0.01

    # Signature regression: should not accept confidence_threshold kwarg
    import inspect
    from detector import detect_animals
    sig = inspect.signature(detect_animals)
    assert "confidence_threshold" not in sig.parameters
```

**Step 2: Run test to verify it fails**

Expected: FAIL — `RAW_CONF_FLOOR` doesn't exist; signature still has `confidence_threshold`.

**Step 3: Implement**

In `vireo/detector.py`:

```python
RAW_CONF_FLOOR = 0.01  # store everything above this; threshold is a read-time filter

def detect_animals(image_path):
    """Run MegaDetector and return every box above RAW_CONF_FLOOR.

    Threshold is applied at read time from workspace-effective config — don't
    filter at write time or we can't globally cache the result.
    """
    session = _get_session()
    if session is None:
        return []
    ...
    return _postprocess(outputs, preprocess_info, RAW_CONF_FLOOR)
```

(Delete the `confidence_threshold` param everywhere in `detector.py`. `_postprocess` keeps its param for internal use; callers just always pass `RAW_CONF_FLOOR`.)

**Step 4: Run test to verify it passes**

```
python -m pytest vireo/tests/test_detector.py -v
```

Expected: PASS. Several existing tests may break — fix them to drop the `confidence_threshold` kwarg.

**Step 5: Commit**

```
git add vireo/detector.py vireo/tests/test_detector.py
git commit -m "detector: hard-floor at 0.01, remove per-call confidence_threshold"
```

---

### Task 15: `db.save_detections` clear-and-reinsert semantics

**Files:**
- Modify: `vireo/db.py` (`save_detections` around line 3815)
- Test: `vireo/tests/test_db.py`

**Step 1: Write failing test**

```python
def test_save_detections_replaces_existing(tmp_path):
    from db import Database
    db = Database(str(tmp_path / "test.db"))
    folder_id = db.add_folder("/tmp/p")
    ws = db.create_workspace("A")
    db._active_workspace_id = ws
    db.add_workspace_folder(ws, folder_id)
    photo_id = db.add_photo(folder_id, "a.jpg")

    det_a = {"box": {"x": 0, "y": 0, "w": 1, "h": 1}, "confidence": 0.9, "category": "animal"}
    det_b = {"box": {"x": 0.2, "y": 0.2, "w": 0.5, "h": 0.5}, "confidence": 0.7, "category": "animal"}

    # First run: two boxes
    ids_v1 = db.save_detections(photo_id, [det_a, det_b], detector_model="megadetector-v6")
    assert len(ids_v1) == 2

    # Second run on same (photo, model): one box — the old rows should be gone
    ids_v2 = db.save_detections(photo_id, [det_a], detector_model="megadetector-v6")
    rows = db.conn.execute(
        "SELECT id FROM detections WHERE photo_id = ? AND detector_model = ?",
        (photo_id, "megadetector-v6"),
    ).fetchall()
    assert {r["id"] for r in rows} == set(ids_v2)


def test_save_detections_is_global(tmp_path):
    from db import Database
    db = Database(str(tmp_path / "test.db"))
    folder_id = db.add_folder("/tmp/p")
    ws_a = db.create_workspace("A")
    ws_b = db.create_workspace("B")
    db.add_workspace_folder(ws_a, folder_id)
    db.add_workspace_folder(ws_b, folder_id)
    photo_id = db.add_photo(folder_id, "a.jpg")

    db._active_workspace_id = ws_a
    db.save_detections(photo_id,
        [{"box": {"x": 0, "y": 0, "w": 1, "h": 1}, "confidence": 0.9, "category": "animal"}],
        detector_model="megadetector-v6")

    db._active_workspace_id = ws_b
    rows = db.conn.execute(
        "SELECT id FROM detections WHERE photo_id = ?", (photo_id,),
    ).fetchall()
    # Global cache: workspace B sees the row written from A
    assert len(rows) == 1
```

**Step 2: Run tests to verify they fail**

Expected: FAIL.

**Step 3: Implement**

Replace `save_detections` and related helpers:

```python
def save_detections(self, photo_id, detections, detector_model):
    """Replace all detections for (photo_id, detector_model) with the given list.

    Global: no workspace scoping. The model's output is a pure function of
    (photo, model); any workspace re-running the same (photo, model) is a
    bug — callers should short-circuit via `get_detector_run_photo_ids`.

    Args:
        photo_id: the photo
        detections: list of dicts {box: {x,y,w,h}, confidence, category}
        detector_model: required, e.g. "megadetector-v6"
    Returns:
        list of new detection IDs (empty if detections was empty).
    """
    if detector_model is None:
        raise ValueError("detector_model is required")
    self.conn.execute(
        "DELETE FROM detections WHERE photo_id = ? AND detector_model = ?",
        (photo_id, detector_model),
    )
    ids = []
    for det in detections:
        box = det["box"]
        cur = self.conn.execute(
            """INSERT INTO detections
                 (photo_id, detector_model, box_x, box_y, box_w, box_h,
                  detector_confidence, category)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?)""",
            (photo_id, detector_model, box["x"], box["y"], box["w"], box["h"],
             det["confidence"], det.get("category", "animal")),
        )
        ids.append(cur.lastrowid)
    self.conn.commit()
    return ids
```

Also rewrite `get_existing_detection_photo_ids` to delegate:

```python
def get_existing_detection_photo_ids(self, detector_model="megadetector-v6"):
    """Back-compat shim — prefer get_detector_run_photo_ids."""
    return self.get_detector_run_photo_ids(detector_model)
```

Delete `clear_detections` workspace-scoped WHERE clause and replace with:

```python
def clear_detections(self, photo_id, detector_model=None):
    if detector_model is None:
        self.conn.execute("DELETE FROM detections WHERE photo_id = ?", (photo_id,))
    else:
        self.conn.execute(
            "DELETE FROM detections WHERE photo_id = ? AND detector_model = ?",
            (photo_id, detector_model),
        )
    self.conn.commit()
```

**Step 4: Run tests to verify they pass**

```
python -m pytest vireo/tests/test_db.py -k "save_detections or clear_detections or detection_photo_ids" -v
```

Expected: PASS. Fix any other `test_db.py` / `test_photos_api.py` failures caused by the signature change.

**Step 5: Commit**

```
git add vireo/db.py vireo/tests/test_db.py
git commit -m "db: save_detections is global + clear-and-reinsert per (photo, model)"
```

---

### Task 16: `classify_job._detect_batch` uses `detector_runs`

**Files:**
- Modify: `vireo/classify_job.py` (around lines 163–338)
- Test: `vireo/tests/test_classify_job.py`

**Step 1: Write failing test**

```python
def test_detect_batch_skips_empty_photo_on_rerun(tmp_path, monkeypatch):
    """A photo with no animals is recorded in detector_runs; rerun skips detection."""
    from db import Database
    db = Database(str(tmp_path / "test.db"))
    folder_id = db.add_folder("/tmp/p")
    ws = db.create_workspace("A")
    db._active_workspace_id = ws
    db.add_workspace_folder(ws, folder_id)
    photo_id = db.add_photo(folder_id, "empty.jpg")

    call_count = {"n": 0}
    def fake_detect(image_path):
        call_count["n"] += 1
        return []  # no animals

    monkeypatch.setattr("classify_job.detect_animals", fake_detect)
    monkeypatch.setattr("classify_job.get_primary_detection", lambda dets: None)

    import classify_job
    photos = [{"id": photo_id, "folder_id": folder_id, "filename": "empty.jpg"}]
    folders = {folder_id: "/tmp/p"}

    # First call: runs detection
    classify_job._detect_batch(
        photos, folders, runner=None, job={"id": 0}, reclassify=False, db=db,
        det_conf_threshold=0.2,
        already_detected_ids=db.get_detector_run_photo_ids("megadetector-v6"),
    )
    assert call_count["n"] == 1

    # Second call: should skip because detector_runs has the row
    classify_job._detect_batch(
        photos, folders, runner=None, job={"id": 0}, reclassify=False, db=db,
        det_conf_threshold=0.2,
        already_detected_ids=db.get_detector_run_photo_ids("megadetector-v6"),
    )
    assert call_count["n"] == 1, "detect_animals should not be re-called for empty photos"
```

**Step 2: Run test to verify it fails**

Expected: FAIL — no detector_runs row is written when the detection list is empty, so the skip check misses.

**Step 3: Implement**

In `vireo/classify_job.py::_detect_batch`, after the `detect_animals(image_path, ...)` call and the branching on empty vs non-empty result, always record the run and always save (even empty):

```python
detections = detect_animals(image_path)  # signature now has no threshold

# Save detections + record the run. Both cases — boxes-found and
# empty-scene — need the detector_runs row so reruns skip.
if detections:
    detected += 1
    db.save_detections(photo["id"], detections, detector_model="megadetector-v6")
    det_list = ...  # existing build
    detection_map[photo["id"]] = det_list
else:
    # Intentionally clear any stale rows for this (photo, model) and
    # record the run with box_count=0.
    db.save_detections(photo["id"], [], detector_model="megadetector-v6")

db.record_detector_run(
    photo["id"], "megadetector-v6", box_count=len(detections)
)

processed_ids.add(photo["id"])
```

Also update the top-level skip check:

```python
if not reclassify and photo["id"] in already_detected_ids:
    # Either an earlier run produced rows, or it ran and found nothing —
    # either way, don't invoke the model again. Pull cached rows if the
    # caller wants them.
    existing = db.get_detections(photo["id"], min_conf=0)  # raw, no filter
    if existing:
        det_list = [...build...]
        detection_map[photo["id"]] = det_list
        detected += 1
    processed_ids.add(photo["id"])
    continue
```

(`db.get_detections` with `min_conf=0` — Task 20 adds the param; for now, keep the existing signature and add the threshold later.)

**Step 4: Run test to verify it passes**

Expected: PASS.

**Step 5: Commit**

```
git add vireo/classify_job.py vireo/tests/test_classify_job.py
git commit -m "classify: use detector_runs to skip re-detecting empty-scene photos"
```

---

### Task 17: `pipeline_job._detect_batch` mirrors Task 16

**Files:**
- Modify: `vireo/pipeline_job.py` (around lines 1283–1358)
- Test: `vireo/tests/test_pipeline_job.py`

**Step 1: Write failing test**

Same shape as Task 16, but driving `pipeline_job._detect_batch`. Copy/adapt.

**Step 2–4:** Apply the same pattern: `detect_animals(image_path)` → always call `db.save_detections` + `db.record_detector_run`. Update the `already_detected` seeding at the top of the function to use `db.get_detector_run_photo_ids`.

**Step 5: Commit**

```
git add vireo/pipeline_job.py vireo/tests/test_pipeline_job.py
git commit -m "pipeline: use detector_runs to skip re-detecting empty-scene photos"
```

---

## Phase 3 — Classifier write path

### Task 18: Compute `labels_fingerprint` at classifier invocation

**Files:**
- Modify: `vireo/classify_job.py` (wherever a classifier is invoked — `_classify_detections` and similar). Also `vireo/pipeline_job.py` for the pipeline version.
- Test: `vireo/tests/test_classify_job.py`

**Step 1: Write failing test**

```python
def test_classifier_fingerprint_upserted(tmp_path, monkeypatch):
    """When a classifier runs, the labels fingerprint is upserted."""
    from db import Database
    db = Database(str(tmp_path / "test.db"))
    folder_id = db.add_folder("/tmp/p")
    ws = db.create_workspace("A")
    db._active_workspace_id = ws
    db.add_workspace_folder(ws, folder_id)
    photo_id = db.add_photo(folder_id, "a.jpg")
    det_ids = db.save_detections(
        photo_id,
        [{"box": {"x": 0, "y": 0, "w": 1, "h": 1}, "confidence": 0.9, "category": "animal"}],
        detector_model="megadetector-v6",
    )

    from labels_fingerprint import compute_fingerprint
    labels = ["Robin", "Sparrow"]
    expected_fp = compute_fingerprint(labels)

    import classify_job
    classify_job._record_labels_fingerprint(
        db, fingerprint=expected_fp, labels=labels,
        sources=["/tmp/active.txt"],
    )
    row = db.conn.execute(
        "SELECT display_name, label_count FROM labels_fingerprints WHERE fingerprint=?",
        (expected_fp,),
    ).fetchone()
    assert row["label_count"] == 2
```

**Step 2: Run test to verify it fails**

Expected: FAIL.

**Step 3: Implement**

Add to `vireo/classify_job.py`:

```python
def _record_labels_fingerprint(db, fingerprint, labels, sources):
    """Populate the labels_fingerprints sidecar. Cosmetic — powers UX lookups."""
    display = ", ".join(os.path.basename(s) for s in (sources or [])) or None
    db.upsert_labels_fingerprint(
        fingerprint=fingerprint,
        display_name=display,
        sources=sources,
        label_count=len(labels or []),
    )
```

Wire it into the classifier flow: immediately after `_load_labels` returns, compute `compute_fingerprint(labels)` and call `_record_labels_fingerprint(db, fp, labels, sources=...)`. Keep `fp` in scope for Task 19.

**Step 4: Run test to verify it passes**

Expected: PASS.

**Step 5: Commit**

```
git add vireo/classify_job.py vireo/tests/test_classify_job.py
git commit -m "classify: compute+record labels fingerprint on classifier run"
```

---

### Task 19: Gate classifier work on `classifier_runs`

**Files:**
- Modify: `vireo/classify_job.py` and `vireo/pipeline_job.py`
- Test: `vireo/tests/test_classify_job.py`

**Step 1: Write failing test**

```python
def test_classifier_skipped_when_run_already_recorded(tmp_path, monkeypatch):
    """If (detection, classifier_model, fingerprint) already ran, don't invoke again."""
    from db import Database
    db = Database(str(tmp_path / "test.db"))
    folder_id = db.add_folder("/tmp/p")
    ws = db.create_workspace("A")
    db._active_workspace_id = ws
    db.add_workspace_folder(ws, folder_id)
    photo_id = db.add_photo(folder_id, "a.jpg")
    det_ids = db.save_detections(
        photo_id,
        [{"box": {"x": 0, "y": 0, "w": 1, "h": 1}, "confidence": 0.9, "category": "animal"}],
        detector_model="megadetector-v6",
    )
    det_id = det_ids[0]

    # Pre-seed a classifier run — any subsequent invocation should bail
    db.record_classifier_run(det_id, "bioclip-2", "abc123", prediction_count=0)

    calls = {"n": 0}
    def fake_classify(*a, **kw):
        calls["n"] += 1
        return []
    monkeypatch.setattr("classify_job._run_classifier_on_detection", fake_classify)

    import classify_job
    classify_job._classify_detection_gated(
        db=db, detection_id=det_id,
        classifier_model="bioclip-2",
        labels_fingerprint="abc123",
        labels=["Robin"], reclassify=False,
    )
    assert calls["n"] == 0, "classifier should be skipped when run key exists"
```

**Step 2: Run test to verify it fails**

Expected: FAIL — `_classify_detection_gated` doesn't exist.

**Step 3: Implement**

Add to `vireo/classify_job.py`:

```python
def _classify_detection_gated(db, detection_id, classifier_model,
                                labels_fingerprint, labels, reclassify):
    """Run the classifier only if we haven't already for this triple."""
    if not reclassify:
        existing = db.get_classifier_run_keys(detection_id)
        if (classifier_model, labels_fingerprint) in existing:
            return []
    predictions = _run_classifier_on_detection(
        db, detection_id, classifier_model, labels,
        labels_fingerprint=labels_fingerprint,
    )
    db.record_classifier_run(
        detection_id, classifier_model, labels_fingerprint,
        prediction_count=len(predictions),
    )
    return predictions
```

Route all existing classifier invocation sites through this helper. `_run_classifier_on_detection` replaces the ad-hoc body that currently builds `predictions` rows and calls `db.conn.execute("INSERT INTO predictions ...")`. That insert must now include `classifier_model` and `labels_fingerprint`.

**Step 4: Run test to verify it passes**

Expected: PASS.

**Step 5: Commit**

```
git add vireo/classify_job.py vireo/tests/test_classify_job.py
git commit -m "classify: gate classifier work on classifier_runs (det, model, fingerprint)"
```

Also apply the same shape to `vireo/pipeline_job.py` in the same commit or as a follow-up commit `pipeline: gate classifier work on classifier_runs`.

---

## Phase 4 — Read-time threshold filters

### Task 20: `db.get_detections(min_conf)` resolves from workspace

**Files:**
- Modify: `vireo/db.py`
- Test: `vireo/tests/test_db.py`

**Step 1: Write failing test**

```python
def test_get_detections_threshold_filter(tmp_path):
    from db import Database
    import config as cfg
    db = Database(str(tmp_path / "test.db"))
    folder_id = db.add_folder("/tmp/p")
    ws = db.create_workspace("A")
    db._active_workspace_id = ws
    db.add_workspace_folder(ws, folder_id)
    photo_id = db.add_photo(folder_id, "a.jpg")
    db.save_detections(photo_id, [
        {"box": {"x": 0, "y": 0, "w": 1, "h": 1}, "confidence": 0.05, "category": "animal"},
        {"box": {"x": 0.1, "y": 0.1, "w": 0.5, "h": 0.5}, "confidence": 0.4, "category": "animal"},
    ], detector_model="megadetector-v6")

    # min_conf=0: returns everything
    rows = db.get_detections(photo_id, min_conf=0)
    assert len(rows) == 2

    # min_conf=0.2: only the 0.4 row
    rows = db.get_detections(photo_id, min_conf=0.2)
    assert len(rows) == 1

    # min_conf=None pulls from workspace-effective config (default 0.2)
    rows = db.get_detections(photo_id)  # min_conf defaults resolved from config
    assert len(rows) == 1


def test_get_detections_cross_workspace_read(tmp_path):
    from db import Database
    db = Database(str(tmp_path / "test.db"))
    folder_id = db.add_folder("/tmp/p")
    ws_a = db.create_workspace("A")
    ws_b = db.create_workspace("B")
    db.add_workspace_folder(ws_a, folder_id)
    db.add_workspace_folder(ws_b, folder_id)
    photo_id = db.add_photo(folder_id, "a.jpg")

    db._active_workspace_id = ws_a
    db.save_detections(photo_id, [
        {"box": {"x": 0, "y": 0, "w": 1, "h": 1}, "confidence": 0.9, "category": "animal"},
    ], detector_model="megadetector-v6")

    db._active_workspace_id = ws_b
    rows = db.get_detections(photo_id, min_conf=0)
    assert len(rows) == 1
```

**Step 2: Run test to verify it fails**

Expected: FAIL.

**Step 3: Implement**

Replace existing `get_detections`:

```python
def get_detections(self, photo_id, min_conf=None, detector_model=None):
    """Return all boxes for a photo above `min_conf`, globally.

    `min_conf=None` pulls `detector_confidence` from workspace-effective config.
    `min_conf=0` returns raw. `detector_model=None` returns all models.
    """
    if min_conf is None:
        import config as cfg
        effective = self.get_effective_config(cfg.load())
        min_conf = effective.get("detector_confidence", 0.2)
    q = ("SELECT * FROM detections WHERE photo_id = ? "
         "AND detector_confidence >= ?")
    params = [photo_id, min_conf]
    if detector_model is not None:
        q += " AND detector_model = ?"
        params.append(detector_model)
    q += " ORDER BY detector_confidence DESC"
    return self.conn.execute(q, params).fetchall()
```

**Step 4: Run tests to verify they pass**

Expected: PASS.

**Step 5: Commit**

```
git add vireo/db.py vireo/tests/test_db.py
git commit -m "db: get_detections applies threshold at read time, globally"
```

---

### Task 21: `db.get_detections_for_photos(min_conf)`

Same pattern as Task 20, batch form. Update `vireo/db.py::get_detections_for_photos` to drop `workspace_id` from its WHERE clause and accept `min_conf=None`. Test: cross-workspace read + threshold filter over a batch.

**Commit:** `db: get_detections_for_photos applies threshold at read time, globally`

---

### Task 22: `db.get_predictions_for_detection(classifier filters)`

**Files:**
- Modify: `vireo/db.py`
- Test: `vireo/tests/test_db.py`

**Step 1: Write failing test**

```python
def test_get_predictions_for_detection_filters(tmp_path):
    from db import Database
    db = Database(str(tmp_path / "test.db"))
    folder_id = db.add_folder("/tmp/p")
    ws = db.create_workspace("A")
    db._active_workspace_id = ws
    db.add_workspace_folder(ws, folder_id)
    photo_id = db.add_photo(folder_id, "a.jpg")
    det_id = db.save_detections(photo_id, [
        {"box": {"x": 0, "y": 0, "w": 1, "h": 1}, "confidence": 0.9, "category": "animal"},
    ], detector_model="megadetector-v6")[0]

    for sp, conf, fp in [("Robin", 0.8, "abc"), ("Sparrow", 0.3, "abc"),
                          ("Robin", 0.9, "xyz")]:
        db.conn.execute(
            """INSERT INTO predictions (detection_id, classifier_model,
                                         labels_fingerprint, species, confidence)
               VALUES (?, 'bioclip-2', ?, ?, ?)""",
            (det_id, fp, sp, conf),
        )
    db.conn.commit()

    # All three rows when unfiltered
    assert len(db.get_predictions_for_detection(det_id, min_classifier_conf=0)) == 3
    # Only ≥ 0.5
    assert len(db.get_predictions_for_detection(det_id, min_classifier_conf=0.5)) == 2
    # Filter by fingerprint
    by_abc = db.get_predictions_for_detection(det_id, labels_fingerprint="abc", min_classifier_conf=0)
    assert {r["species"] for r in by_abc} == {"Robin", "Sparrow"}
```

**Step 2–4:** Implement:

```python
def get_predictions_for_detection(self, detection_id,
                                    min_classifier_conf=None,
                                    classifier_model=None,
                                    labels_fingerprint=None):
    if min_classifier_conf is None:
        import config as cfg
        effective = self.get_effective_config(cfg.load())
        min_classifier_conf = effective.get("classifier_confidence", 0.0)
    q = ("SELECT * FROM predictions WHERE detection_id = ? "
         "AND confidence >= ?")
    params = [detection_id, min_classifier_conf]
    if classifier_model is not None:
        q += " AND classifier_model = ?"
        params.append(classifier_model)
    if labels_fingerprint is not None:
        q += " AND labels_fingerprint = ?"
        params.append(labels_fingerprint)
    q += " ORDER BY confidence DESC"
    return self.conn.execute(q, params).fetchall()
```

**Step 5: Commit**

```
git add vireo/db.py vireo/tests/test_db.py
git commit -m "db: get_predictions_for_detection with model + fingerprint filters"
```

---

### Task 23: Update `app.py` detection endpoints to use new filters

**Files:**
- Modify: `vireo/app.py` — search for `detections` queries (roughly lines 6433, 7042 per earlier grep).
- Test: `vireo/tests/test_photos_api.py`

**Step 1: Identify all call sites**

Run:

```
grep -nE "FROM detections|db\.get_detections|get_detections_for_photos" vireo/app.py
```

Update every `WHERE workspace_id = ?` to drop the clause; if the query needs a threshold filter, use the new helpers instead of inline SQL.

**Step 2: Adjust tests + API to assert read-time filtering works**

Pick one representative test in `vireo/tests/test_photos_api.py` that exercises `/api/photos/<id>/detections` or similar, and assert that lowering `detector_confidence` in workspace config changes the returned count without rewriting any rows.

**Step 3: Run the full test suite**

```
python -m pytest vireo/tests/ tests/ -v
```

**Step 4: Commit**

```
git add vireo/app.py vireo/tests/test_photos_api.py
git commit -m "api: route detection/prediction reads through global threshold helpers"
```

---

### Task 24: Update `pipeline.py` subject-crop queries

**Files:**
- Modify: `vireo/pipeline.py` (subject-crop extraction + detection-driven queries)
- Test: `vireo/tests/test_pipeline.py`

Walk every `SELECT ... FROM detections` / `JOIN detections`, drop `workspace_id` predicates, and add `detector_confidence >= ?` resolved from the workspace-effective config. Commit:

```
git add vireo/pipeline.py vireo/tests/test_pipeline.py
git commit -m "pipeline: filter detections at read time; drop workspace scoping"
```

---

### Task 25: Update stats / aggregation queries

**Files:**
- Modify: `vireo/db.py` (all `*stats*`, `get_photos_with_detections_but_no_masks`, `get_photos_by_prediction`, `get_species_counts`, etc.)
- Test: `vireo/tests/test_db.py`

For each query, the pattern is the same:

- Drop `detections.workspace_id = ?` (it no longer exists).
- Keep `workspace_folders.workspace_id = ?` to scope *photos* to the active workspace.
- Add `detections.detector_confidence >= ?` (or predictions equivalent) with threshold resolved from workspace config.
- For prediction review joins, `LEFT JOIN prediction_review pr ON pr.prediction_id = p.id AND pr.workspace_id = ?` and treat `pr.status IS NULL` as `'pending'`.

Landmarks to convert (from the earlier grep):
- `get_prediction_stats` (line ~2282)
- `get_photos_by_prediction` (line ~2300)
- `get_species_counts` (line ~2441)
- `get_folders_with_quality_data` (line ~2871)
- `get_detection_stats` helpers

Commit:

```
git add vireo/db.py vireo/tests/test_db.py
git commit -m "db: workspace-scope queries through workspace_folders only; read-time threshold"
```

---

## Phase 5 — Polish

### Task 26: Settings stats: "Detections: N photos × M models cached"

**Files:**
- Modify: `vireo/app.py` (stats/settings endpoint)
- Modify: `vireo/templates/settings.html`
- Test: `vireo/tests/test_app.py`

Add `db.get_global_detection_stats()`:

```python
def get_global_detection_stats(self):
    r = self.conn.execute(
        """SELECT COUNT(DISTINCT photo_id) AS photo_count,
                  COUNT(DISTINCT detector_model) AS model_count
           FROM detector_runs"""
    ).fetchone()
    return {"photo_count": r["photo_count"] or 0,
            "model_count": r["model_count"] or 0}
```

Render as "N photos × M models cached" on the settings page. Test asserts the number changes after a scan.

Commit:

```
git add vireo/db.py vireo/app.py vireo/templates/settings.html vireo/tests/test_app.py
git commit -m "settings: show global detection cache stats"
```

---

### Task 27: Changelog + headless API audit

**Files:**
- Modify: `CHANGELOG.md` or equivalent release notes file
- Modify: Any headless API handlers that expose `predictions.status` / `individual` / `group_id` — those now read from `prediction_review`.

**Step 1:** Grep:

```
grep -nE "status.*prediction|prediction.*status|\.individual|\.group_id" vireo/app.py
```

**Step 2:** For each hit, route through `db.get_review_status` / `db.set_review_status`.

**Step 3:** Add changelog entry:

```markdown
### Changed
- **Global detection/classifier cache.** MegaDetector and classifier results
  are now cached per-photo instead of per-workspace. Switching to a new
  workspace or changing your detector confidence threshold no longer
  triggers a full reprocess.
- **Threshold is now a read-time filter.** Lowering `detector_confidence` in
  workspace config takes effect immediately; you no longer need to rerun
  detection to see previously-subthreshold boxes.
- Legacy detections from prior versions are preserved but pre-filtered. Run
  "Reclassify" once per folder to regenerate them with the new raw storage
  if you want to take full advantage of low-threshold browsing.
```

**Step 4:** Full test suite:

```
python -m pytest tests/ vireo/tests/ -v
```

Expected: all green.

**Step 5: Commit**

```
git add CHANGELOG.md vireo/app.py
git commit -m "docs: changelog for global detection cache; migrate headless API reads to prediction_review"
```

---

## Final verification

Before opening a PR:

1. Full pytest: `python -m pytest tests/ vireo/tests/ -v`. Expected: all pass.
2. Hand-check: scan a folder in workspace A, create workspace B, point at the same folder, open pipeline. Expected: detection + classify phases skip everything, complete near-instantly.
3. In workspace B, lower `detector_confidence` to 0.05 and reload the browse grid. Expected: more boxes visible immediately, no background job fires.
4. `gh pr create` against `main`, include the design doc path in the PR description.

## References

- Design: `docs/plans/2026-04-23-detection-storage-redesign-design.md`
- Key files by phase:
  - Phase 0/1: `vireo/db.py`, `vireo/tests/test_db.py`
  - Phase 2: `vireo/detector.py`, `vireo/classify_job.py`, `vireo/pipeline_job.py`
  - Phase 3: `vireo/classify_job.py`, `vireo/pipeline_job.py`, `vireo/labels_fingerprint.py`
  - Phase 4: `vireo/db.py`, `vireo/app.py`, `vireo/pipeline.py`
  - Phase 5: `vireo/app.py`, `vireo/templates/settings.html`, `CHANGELOG.md`
