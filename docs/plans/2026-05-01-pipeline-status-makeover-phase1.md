# Pipeline Status Makeover — Phase 1 Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Backend-only foundation for the pipeline status makeover — add per-stage fingerprint columns, write `db.pipeline_stage_counts()` returning the new `stages` block, wire it into `/api/pipeline/page-init` alongside existing fields. Zero UI changes; existing UI keeps working unchanged.

**Architecture:** Two new columns (`photos.eye_kp_fingerprint`, plus `workspaces.last_grouped_at` + `last_group_fingerprint`) carry settings provenance for stages that don't already have one. Backfilled to the current fingerprint so existing data renders as "fresh", not "outdated". A new `pipeline_stage_counts()` aggregates per-stage `done`/`stale`/`eligible_total` numbers using existing tables (`detector_runs`, `classifier_runs`, `photos`, `workspace_folders`). Extract is deliberately omitted — that block depends on PR #736's `photo_masks` table and lands in Phase 3.

**Tech Stack:** SQLite (WAL), Flask, pytest. Existing `db.get_pipeline_feature_counts()` is the closest model to study; new helper follows the same workspace-scoping pattern (`self._ws_id()` + `JOIN workspace_folders wf`).

**Reference design:** `docs/plans/2026-05-01-pipeline-status-makeover-design.md`.

**Test command (run after each Python-touching task):**
```bash
python -m pytest vireo/tests/test_db.py vireo/tests/test_app.py -v
```

---

## Task 1.1: Add `photos.eye_kp_fingerprint` column

**Files:**
- Modify: `vireo/db.py` lines 223-276 (`CREATE TABLE photos`) and the migration block around line 627-642
- Test: `vireo/tests/test_db.py`

**Step 1: Write the failing test**

Add to `vireo/tests/test_db.py`:

```python
def test_photos_has_eye_kp_fingerprint_column(tmp_path):
    """photos.eye_kp_fingerprint must exist on a fresh DB."""
    from db import Database
    db = Database(str(tmp_path / "v.db"))
    db.conn.execute("SELECT eye_kp_fingerprint FROM photos LIMIT 0")


def test_photos_eye_kp_fingerprint_migrates_on_old_db(tmp_path):
    """Opening a DB without the column adds it (idempotent migration)."""
    import sqlite3
    from db import Database
    p = str(tmp_path / "v.db")
    db = Database(p)
    db.conn.execute("ALTER TABLE photos DROP COLUMN eye_kp_fingerprint")
    db.conn.commit()
    db.close()
    db2 = Database(p)
    db2.conn.execute("SELECT eye_kp_fingerprint FROM photos LIMIT 0")
```

**Step 2: Run failing**

```bash
python -m pytest vireo/tests/test_db.py::test_photos_has_eye_kp_fingerprint_column -v
```

Expected: FAIL with `no such column: eye_kp_fingerprint`.

**Step 3: Implement**

In `vireo/db.py` line 270 (in the `CREATE TABLE photos` block, after `eye_tenengrad REAL,`), add:

```sql
                eye_kp_fingerprint       TEXT,
```

In the migration block (after the `working_copy_failed_mtime` block at line 642), add:

```python
        # Migration: add eye_kp_fingerprint column. Set to NULL for new
        # photos; populated when the eye-keypoint stage runs. Phase 1 also
        # backfills existing eye-keypoint rows to the current fingerprint
        # in a separate migration step (see Task 2.1).
        try:
            self.conn.execute("SELECT eye_kp_fingerprint FROM photos LIMIT 0")
        except sqlite3.OperationalError:
            self.conn.execute(
                "ALTER TABLE photos ADD COLUMN eye_kp_fingerprint TEXT"
            )
```

**Step 4: Run passing**

```bash
python -m pytest vireo/tests/test_db.py::test_photos_has_eye_kp_fingerprint_column vireo/tests/test_db.py::test_photos_eye_kp_fingerprint_migrates_on_old_db -v
```

Expected: PASS.

**Step 5: Commit**

```bash
git add vireo/db.py vireo/tests/test_db.py
git commit -m "db: add photos.eye_kp_fingerprint column"
```

---

## Task 1.2: Add `workspaces.last_grouped_at` and `last_group_fingerprint` columns

**Files:**
- Modify: `vireo/db.py` lines 315-323 (`CREATE TABLE workspaces`) and the migration block
- Test: `vireo/tests/test_db.py`

**Step 1: Write the failing test**

```python
def test_workspaces_has_group_state_columns(tmp_path):
    from db import Database
    db = Database(str(tmp_path / "v.db"))
    db.conn.execute(
        "SELECT last_grouped_at, last_group_fingerprint FROM workspaces LIMIT 0"
    )
```

**Step 2: Run failing.** Expected: FAIL with `no such column: last_grouped_at`.

**Step 3: Implement**

In the `CREATE TABLE workspaces` block at line 315, after `last_opened_at TEXT`, add:

```sql
                last_grouped_at         INTEGER,
                last_group_fingerprint  TEXT,
```

In the migration block, append (after the `tabs` migration at line 614-620):

```python
        # Migration: per-workspace grouping provenance. last_grouped_at is
        # the unix epoch when run_full_pipeline last completed for this
        # workspace; last_group_fingerprint is a stable hash of the encounter
        # + burst params used. Both NULL for fresh workspaces.
        try:
            self.conn.execute("SELECT last_grouped_at FROM workspaces LIMIT 0")
        except sqlite3.OperationalError:
            self.conn.execute(
                "ALTER TABLE workspaces ADD COLUMN last_grouped_at INTEGER"
            )
        try:
            self.conn.execute("SELECT last_group_fingerprint FROM workspaces LIMIT 0")
        except sqlite3.OperationalError:
            self.conn.execute(
                "ALTER TABLE workspaces ADD COLUMN last_group_fingerprint TEXT"
            )
```

**Step 4: Run passing.** Same test command, PASS.

**Step 5: Commit**

```bash
git add vireo/db.py vireo/tests/test_db.py
git commit -m "db: add workspaces.last_grouped_at + last_group_fingerprint columns"
```

---

## Task 1.3: Define `EYE_KP_FINGERPRINT_VERSION` and `compute_group_fingerprint`

**Files:**
- Modify: `vireo/pipeline.py` near line 679 (next to `_EYE_KEYPOINT_MODEL_FOR_CLASS`)
- Test: `vireo/tests/test_pipeline.py`

**Step 1: Write the failing test**

Add to `vireo/tests/test_pipeline.py`:

```python
def test_eye_kp_fingerprint_version_is_string():
    from pipeline import EYE_KP_FINGERPRINT_VERSION
    assert isinstance(EYE_KP_FINGERPRINT_VERSION, str)
    assert len(EYE_KP_FINGERPRINT_VERSION) > 0


def test_compute_group_fingerprint_is_stable_for_same_input():
    from pipeline import compute_group_fingerprint
    cfg = {"pipeline": {"foo": 1}}
    assert compute_group_fingerprint(cfg) == compute_group_fingerprint(cfg)


def test_compute_group_fingerprint_changes_with_encounter_defaults():
    """Bumping any value in encounters.DEFAULTS must change the fingerprint."""
    import importlib
    import encounters
    from pipeline import compute_group_fingerprint
    cfg = {}
    fp_before = compute_group_fingerprint(cfg)
    original = encounters.DEFAULTS.copy()
    try:
        encounters.DEFAULTS["w_time"] = original["w_time"] + 0.01
        fp_after = compute_group_fingerprint(cfg)
        assert fp_after != fp_before
    finally:
        encounters.DEFAULTS.clear()
        encounters.DEFAULTS.update(original)
```

**Step 2: Run failing**

```bash
python -m pytest vireo/tests/test_pipeline.py::test_eye_kp_fingerprint_version_is_string -v
```

Expected: FAIL with `cannot import name 'EYE_KP_FINGERPRINT_VERSION'`.

**Step 3: Implement**

In `vireo/pipeline.py` after line 681 (after the `_EYE_KEYPOINT_MODEL_FOR_CLASS` dict), add:

```python
# Bump this string when the eye-keypoint routing or weights change in a
# way that invalidates previously persisted (eye_x, eye_y, eye_conf,
# eye_tenengrad) values. The pipeline page reads photos.eye_kp_fingerprint
# and treats != current as "Outdated".
EYE_KP_FINGERPRINT_VERSION = "v1"


def compute_group_fingerprint(_config):
    """Stable hash of the params that drive encounter + burst grouping.

    Workspaces store the fingerprint observed at the last completed
    grouping run; the pipeline page treats != current as "Outdated".
    The _config arg is reserved for future per-workspace overrides; for
    now grouping uses module-level DEFAULTS only.
    """
    import hashlib
    import json
    import bursts
    import encounters
    payload = {
        "encounters": dict(encounters.DEFAULTS),
        "bursts": dict(bursts.DEFAULTS),
    }
    blob = json.dumps(payload, sort_keys=True).encode("utf-8")
    return hashlib.sha1(blob).hexdigest()[:16]
```

**Step 4: Run passing**

```bash
python -m pytest vireo/tests/test_pipeline.py -k "fingerprint" -v
```

Expected: 3 PASS.

**Step 5: Commit**

```bash
git add vireo/pipeline.py vireo/tests/test_pipeline.py
git commit -m "pipeline: define eye-kp + group fingerprint constants"
```

---

## Task 2.1: Backfill `eye_kp_fingerprint` on existing rows during migration

This makes the migration friendly — existing eye-keypoint data renders as "fresh" instead of "Outdated" when the column ships.

**Files:**
- Modify: `vireo/db.py` migration block (the one added in Task 1.1)
- Test: `vireo/tests/test_db.py`

**Step 1: Write the failing test**

```python
def test_eye_kp_fingerprint_backfilled_on_migration(tmp_path):
    """Existing photos with eye_tenengrad NOT NULL get the current
    fingerprint stamped in during migration; clean rows stay NULL."""
    import sqlite3
    from db import Database
    from pipeline import EYE_KP_FINGERPRINT_VERSION
    p = str(tmp_path / "v.db")
    db = Database(p)
    db.conn.execute("INSERT INTO folders(path) VALUES ('/tmp')")
    db.conn.execute(
        "INSERT INTO photos(id, folder_id, filename, eye_tenengrad) "
        "VALUES (1, 1, 'a.jpg', 12.5), (2, 1, 'b.jpg', NULL)"
    )
    db.conn.execute(
        "UPDATE photos SET eye_kp_fingerprint = NULL WHERE id IN (1, 2)"
    )
    db.conn.commit()
    db.close()
    db2 = Database(p)
    rows = dict(db2.conn.execute(
        "SELECT id, eye_kp_fingerprint FROM photos ORDER BY id"
    ).fetchall())
    assert rows[1]["eye_kp_fingerprint"] == EYE_KP_FINGERPRINT_VERSION
    assert rows[2]["eye_kp_fingerprint"] is None
```

Wait — this test is wrong because the migration only runs once (column already exists on second open). Adjust:

Replace the test with an idempotent backfill marker — use `db_meta` (the singleton key/value table at line 310):

```python
def test_eye_kp_fingerprint_backfilled_on_migration(tmp_path):
    """One-shot backfill: photos with eye_tenengrad NOT NULL get the
    current fingerprint; rows without eye data stay NULL. Repeated DB
    opens don't re-stamp."""
    import sqlite3
    from db import Database
    from pipeline import EYE_KP_FINGERPRINT_VERSION
    p = str(tmp_path / "v.db")
    db = Database(p)
    db.conn.execute("INSERT INTO folders(path) VALUES ('/tmp')")
    db.conn.execute(
        "INSERT INTO photos(id, folder_id, filename, eye_tenengrad, "
        "eye_kp_fingerprint) "
        "VALUES (1, 1, 'a.jpg', 12.5, NULL), (2, 1, 'b.jpg', NULL, NULL)"
    )
    # Wipe the migration marker so reopening reruns the backfill once.
    db.conn.execute("DELETE FROM db_meta WHERE key='eye_kp_fingerprint_backfill'")
    db.conn.commit()
    db.close()
    db2 = Database(p)
    rows = {r["id"]: r["eye_kp_fingerprint"]
            for r in db2.conn.execute(
                "SELECT id, eye_kp_fingerprint FROM photos ORDER BY id"
            ).fetchall()}
    assert rows[1] == EYE_KP_FINGERPRINT_VERSION
    assert rows[2] is None
    # Marker now set; reopening must NOT touch already-populated rows.
    db2.conn.execute(
        "UPDATE photos SET eye_kp_fingerprint = 'mutated' WHERE id = 1"
    )
    db2.conn.commit()
    db2.close()
    db3 = Database(p)
    after = db3.conn.execute(
        "SELECT eye_kp_fingerprint FROM photos WHERE id = 1"
    ).fetchone()[0]
    assert after == "mutated"  # backfill skipped on second open
```

**Step 2: Run failing.** Expected: FAIL — backfill not implemented yet.

**Step 3: Implement**

In `vireo/db.py`, after the `eye_kp_fingerprint` column migration from Task 1.1, add:

```python
        # One-shot backfill: stamp the current EYE_KP_FINGERPRINT_VERSION
        # onto photos that already have eye-keypoint data, so existing
        # users don't see "Outdated" for unchanged data on first upgrade.
        # Gated by db_meta so it runs exactly once per DB.
        marker = self.conn.execute(
            "SELECT value FROM db_meta WHERE key='eye_kp_fingerprint_backfill'"
        ).fetchone()
        if marker is None:
            from pipeline import EYE_KP_FINGERPRINT_VERSION
            self.conn.execute(
                "UPDATE photos SET eye_kp_fingerprint = ? "
                "WHERE eye_tenengrad IS NOT NULL AND eye_kp_fingerprint IS NULL",
                (EYE_KP_FINGERPRINT_VERSION,),
            )
            self.conn.execute(
                "INSERT INTO db_meta(key, value) VALUES ('eye_kp_fingerprint_backfill', '1')"
            )
```

**Step 4: Run passing.** PASS.

**Step 5: Commit**

```bash
git add vireo/db.py vireo/tests/test_db.py
git commit -m "db: backfill eye_kp_fingerprint on first upgrade"
```

---

## Task 2.2: Pipeline writes `eye_kp_fingerprint` after stage completes

Phase 1 needs the write side too — otherwise newly-processed photos start NULL and immediately render as "Outdated" the next page load.

**Files:**
- Modify: `vireo/pipeline.py` around line 834 (the `eye_x=...` upsert in `detect_eye_keypoints_stage`)
- Modify: `vireo/db.py` — wherever the eye-keypoint columns are written (search for `eye_tenengrad=` UPDATE)
- Test: `vireo/tests/test_pipeline.py`

**Step 1: Identify the write site.** Run:

```bash
grep -n "eye_tenengrad" vireo/db.py vireo/pipeline.py
```

Locate the function (likely `db.update_photo_eye_keypoint` or inline in `detect_eye_keypoints_stage`) that writes `eye_x`, `eye_y`, `eye_conf`, `eye_tenengrad`. Add `eye_kp_fingerprint` to the same UPDATE / parameter list. If the function is in `db.py`, accept it as a kwarg defaulting to None and SET it in the UPDATE.

**Step 2: Write the failing test**

```python
def test_eye_kp_stage_stamps_fingerprint(tmp_path, monkeypatch):
    """After detect_eye_keypoints_stage writes a row, eye_kp_fingerprint
    must equal EYE_KP_FINGERPRINT_VERSION."""
    from db import Database
    from pipeline import EYE_KP_FINGERPRINT_VERSION
    db = Database(str(tmp_path / "v.db"))
    db.conn.execute("INSERT INTO folders(path) VALUES ('/tmp')")
    db.conn.execute(
        "INSERT INTO photos(id, folder_id, filename) VALUES (1, 1, 'a.jpg')"
    )
    db.conn.commit()
    # Use whichever helper is at the write site:
    db.update_photo_eye_keypoint(
        photo_id=1, eye_x=0.5, eye_y=0.5, eye_conf=0.9, eye_tenengrad=12.0,
    )
    fp = db.conn.execute(
        "SELECT eye_kp_fingerprint FROM photos WHERE id=1"
    ).fetchone()[0]
    assert fp == EYE_KP_FINGERPRINT_VERSION
```

If the write site is inline in `pipeline.py` (no `update_photo_eye_keypoint` helper), adapt the test to assert via the stage path or extract the helper first.

**Step 2 (cont.): Run failing.** PASS-with-wrong-value or FAIL-with-AttributeError depending on existing shape.

**Step 3: Implement**

In the `db.py` UPDATE that writes eye-keypoint columns, add `eye_kp_fingerprint = ?` to the SET list and pass `EYE_KP_FINGERPRINT_VERSION` from `pipeline.py` (import at the call site, NOT inside `db.py`, to keep `db.py` free of pipeline.py imports).

**Step 4: Run passing.** PASS.

**Step 5: Commit**

```bash
git add vireo/db.py vireo/pipeline.py vireo/tests/test_pipeline.py
git commit -m "pipeline: stamp eye_kp_fingerprint when stage writes keypoints"
```

---

## Task 2.3: Pipeline writes group fingerprint after grouping completes

**Files:**
- Modify: `vireo/pipeline_job.py` around line 2745 (end of the `regroup_stage`) — locate by searching for `compute_misses_for_workspace` and going up
- Modify: `vireo/db.py` — add `set_workspace_group_state(workspace_id, fingerprint, when_ts)` helper
- Test: `vireo/tests/test_db.py` for the helper, `vireo/tests/test_pipeline_job.py` for the wiring

**Step 1: Write the failing test (db helper)**

```python
def test_set_workspace_group_state(tmp_path):
    from db import Database
    db = Database(str(tmp_path / "v.db"))
    ws_id = db.create_workspace("test")
    db.set_workspace_group_state(ws_id, fingerprint="abc123", when_ts=1714579200)
    row = db.conn.execute(
        "SELECT last_grouped_at, last_group_fingerprint FROM workspaces WHERE id=?",
        (ws_id,),
    ).fetchone()
    assert row["last_grouped_at"] == 1714579200
    assert row["last_group_fingerprint"] == "abc123"
```

**Step 2: Run failing.** Expected: `AttributeError: 'Database' object has no attribute 'set_workspace_group_state'`.

**Step 3: Implement**

In `vireo/db.py` near the other workspace helpers (~line 998):

```python
    def set_workspace_group_state(self, workspace_id, fingerprint, when_ts):
        """Record that grouping completed for `workspace_id` at `when_ts`
        with the given `fingerprint`. Pipeline page treats fingerprint
        mismatch as "Outdated" so the user knows a regroup is pending.
        """
        self.conn.execute(
            "UPDATE workspaces SET last_grouped_at = ?, last_group_fingerprint = ? "
            "WHERE id = ?",
            (when_ts, fingerprint, workspace_id),
        )
        self.conn.commit()
```

In `pipeline_job.py` at the end of the regroup stage (where it currently calls `runner.update_step` with `status="completed"`), call:

```python
from pipeline import compute_group_fingerprint
import time
thread_db.set_workspace_group_state(
    workspace_id=active_ws,
    fingerprint=compute_group_fingerprint(config),
    when_ts=int(time.time()),
)
```

Place this BEFORE the `update_step(... status="completed")` so a partial regroup that crashes doesn't stamp a misleading fingerprint.

**Step 4: Run passing.** PASS.

**Step 5: Commit**

```bash
git add vireo/db.py vireo/pipeline_job.py vireo/tests/test_db.py
git commit -m "pipeline: stamp workspace group fingerprint after regroup"
```

---

## Task 3.1: `db.pipeline_stage_counts(workspace_id=None)` — the new helper

The centerpiece. Returns the `stages` block from the design doc, minus `extract` (Phase 3).

**Files:**
- Modify: `vireo/db.py` — add the method near `get_pipeline_feature_counts` at line 2486
- Test: `vireo/tests/test_db.py`

**Step 1: Write the failing test**

```python
def test_pipeline_stage_counts_empty_workspace(tmp_path):
    """Fresh workspace, no photos: every stage shows zeros."""
    from db import Database
    db = Database(str(tmp_path / "v.db"))
    out = db.pipeline_stage_counts()
    assert out["scan"]["done"] == 0
    assert out["scan"]["eligible_total"] == 0
    assert out["previews"]["done"] == 0
    assert out["detect"]["done"] == 0
    assert out["classify"]["done"] == 0
    assert out["eye_kp"]["done"] == 0
    assert out["group"]["computed"] is False
    assert out["group"]["last_at"] is None
    assert "extract" not in out  # deferred to Phase 3


def test_pipeline_stage_counts_scan_and_previews(tmp_path):
    """scan.done counts photos with timestamp + width;
    previews.done counts photos with thumb_path."""
    from db import Database
    db = Database(str(tmp_path / "v.db"))
    db.conn.execute("INSERT INTO folders(path) VALUES ('/tmp')")
    fid = 1
    ws_id = db.get_active_workspace()["id"]
    db.add_workspace_folder(ws_id, fid)
    db.conn.execute(
        "INSERT INTO photos(id, folder_id, filename, timestamp, width, thumb_path) "
        "VALUES (1, ?, 'a.jpg', '2026-01-01', 100, '/t/1.jpg'), "
        "       (2, ?, 'b.jpg', NULL,         100, '/t/2.jpg'), "
        "       (3, ?, 'c.jpg', '2026-01-01', 100, NULL)",
        (fid, fid, fid),
    )
    db.conn.commit()
    out = db.pipeline_stage_counts()
    assert out["scan"]["eligible_total"] == 3
    assert out["scan"]["done"] == 1            # photo 1 only
    assert out["previews"]["done"] == 2        # photos 1 + 2
    assert out["previews"]["eligible_total"] == 3


def test_pipeline_stage_counts_eye_kp_stale(tmp_path):
    """eye_kp.stale counts rows with eye_tenengrad NOT NULL but
    eye_kp_fingerprint != current."""
    from db import Database
    from pipeline import EYE_KP_FINGERPRINT_VERSION
    db = Database(str(tmp_path / "v.db"))
    db.conn.execute("INSERT INTO folders(path) VALUES ('/tmp')")
    fid = 1
    ws_id = db.get_active_workspace()["id"]
    db.add_workspace_folder(ws_id, fid)
    db.conn.execute(
        "INSERT INTO photos(id, folder_id, filename, eye_tenengrad, "
        "eye_kp_fingerprint) VALUES "
        "(1, ?, 'a.jpg', 10.0, ?), "                       # done
        "(2, ?, 'b.jpg', 10.0, 'old-version'), "          # stale
        "(3, ?, 'c.jpg', NULL, NULL)",                    # neither
        (fid, EYE_KP_FINGERPRINT_VERSION, fid, fid),
    )
    db.conn.commit()
    out = db.pipeline_stage_counts()
    assert out["eye_kp"]["done"] == 1
    assert out["eye_kp"]["stale"] == 1


def test_pipeline_stage_counts_group_state(tmp_path):
    from db import Database
    from pipeline import compute_group_fingerprint
    db = Database(str(tmp_path / "v.db"))
    ws_id = db.get_active_workspace()["id"]
    fp = compute_group_fingerprint({})
    db.set_workspace_group_state(ws_id, fingerprint=fp, when_ts=1714579200)
    out = db.pipeline_stage_counts()
    assert out["group"]["computed"] is True
    assert out["group"]["stale"] is False
    assert out["group"]["last_at"] == 1714579200
    db.set_workspace_group_state(ws_id, fingerprint="old", when_ts=1714579200)
    out2 = db.pipeline_stage_counts()
    assert out2["group"]["stale"] is True
```

**Step 2: Run failing.** Expected: `AttributeError: 'Database' object has no attribute 'pipeline_stage_counts'`.

**Step 3: Implement**

In `vireo/db.py` after `get_pipeline_feature_counts` (~line 2520), add:

```python
    def pipeline_stage_counts(self):
        """Per-stage counts for the active workspace, used by the pipeline
        page to render accurate "Will run / Resume / Already done /
        Outdated" pills. See docs/plans/2026-05-01-pipeline-status-makeover-design.md.

        Extract is intentionally omitted — that block depends on PR #736
        (per-variant photo_masks) and lands in Phase 3.
        """
        import config as cfg
        from pipeline import EYE_KP_FINGERPRINT_VERSION, compute_group_fingerprint
        ws = self._ws_id()
        effective = self.get_effective_config(cfg.load())
        min_conf = effective.get("detector_confidence", 0.2)

        # eligible_total + scan.done + previews.done in one pass
        scan_row = self.conn.execute(
            """SELECT
                  COUNT(*) AS eligible,
                  SUM(CASE WHEN p.timestamp IS NOT NULL AND p.width IS NOT NULL
                           THEN 1 ELSE 0 END) AS scan_done,
                  SUM(CASE WHEN p.thumb_path IS NOT NULL THEN 1 ELSE 0 END) AS previews_done
               FROM photos p
               JOIN workspace_folders wf ON wf.folder_id = p.folder_id
              WHERE wf.workspace_id = ?""",
            (ws,),
        ).fetchone()
        eligible = scan_row["eligible"] or 0

        # detect.done — distinct photos with at least one above-threshold non-full-image
        # detection in this workspace.
        detect_done = self.conn.execute(
            """SELECT COUNT(DISTINCT d.photo_id)
                 FROM detections d
                 JOIN photos p ON p.id = d.photo_id
                 JOIN workspace_folders wf ON wf.folder_id = p.folder_id
                WHERE wf.workspace_id = ?
                  AND d.detector_confidence >= ?
                  AND d.detector_model != 'full-image'""",
            (ws, min_conf),
        ).fetchone()[0] or 0

        # classify.done / .stale — uses classifier_runs + current model + fp.
        # If the user has never selected a classifier model the count is 0;
        # the UI then renders "Will run" naturally.
        pipe_cfg = effective.get("pipeline", {})
        cur_model = pipe_cfg.get("classifier_model", "")
        cur_labels_fp = pipe_cfg.get("labels_fingerprint", "")  # set by classifier loader
        classify_done = 0
        classify_stale = 0
        if cur_model and cur_labels_fp:
            classify_done = self.conn.execute(
                """SELECT COUNT(DISTINCT d.photo_id)
                     FROM detections d
                     JOIN classifier_runs cr ON cr.detection_id = d.id
                     JOIN photos p ON p.id = d.photo_id
                     JOIN workspace_folders wf ON wf.folder_id = p.folder_id
                    WHERE wf.workspace_id = ?
                      AND cr.classifier_model = ?
                      AND cr.labels_fingerprint = ?
                      AND d.detector_confidence >= ?
                      AND d.detector_model != 'full-image'""",
                (ws, cur_model, cur_labels_fp, min_conf),
            ).fetchone()[0] or 0
            classify_stale = self.conn.execute(
                """SELECT COUNT(DISTINCT d.photo_id)
                     FROM detections d
                     JOIN classifier_runs cr ON cr.detection_id = d.id
                     JOIN photos p ON p.id = d.photo_id
                     JOIN workspace_folders wf ON wf.folder_id = p.folder_id
                    WHERE wf.workspace_id = ?
                      AND (cr.classifier_model != ? OR cr.labels_fingerprint != ?)
                      AND d.detector_confidence >= ?
                      AND d.detector_model != 'full-image'""",
                (ws, cur_model, cur_labels_fp, min_conf),
            ).fetchone()[0] or 0
            classify_stale = max(0, classify_stale - classify_done)

        # eye_kp.done / .stale
        eye_done = self.conn.execute(
            """SELECT COUNT(*)
                 FROM photos p
                 JOIN workspace_folders wf ON wf.folder_id = p.folder_id
                WHERE wf.workspace_id = ?
                  AND p.eye_tenengrad IS NOT NULL
                  AND p.eye_kp_fingerprint = ?""",
            (ws, EYE_KP_FINGERPRINT_VERSION),
        ).fetchone()[0] or 0
        eye_stale = self.conn.execute(
            """SELECT COUNT(*)
                 FROM photos p
                 JOIN workspace_folders wf ON wf.folder_id = p.folder_id
                WHERE wf.workspace_id = ?
                  AND p.eye_tenengrad IS NOT NULL
                  AND (p.eye_kp_fingerprint IS NULL OR p.eye_kp_fingerprint != ?)""",
            (ws, EYE_KP_FINGERPRINT_VERSION),
        ).fetchone()[0] or 0

        # group state
        gs_row = self.conn.execute(
            "SELECT last_grouped_at, last_group_fingerprint FROM workspaces WHERE id=?",
            (ws,),
        ).fetchone()
        cur_group_fp = compute_group_fingerprint(effective)
        last_at = gs_row["last_grouped_at"] if gs_row else None
        last_fp = gs_row["last_group_fingerprint"] if gs_row else None
        group_computed = last_fp is not None
        group_stale = group_computed and last_fp != cur_group_fp

        return {
            "scan":     {"done": scan_row["scan_done"] or 0,    "stale": 0, "eligible_total": eligible, "total": eligible},
            "previews": {"done": scan_row["previews_done"] or 0, "stale": 0, "eligible_total": eligible, "total": eligible},
            "detect":   {"done": detect_done, "stale": 0, "eligible_total": eligible, "total": eligible,
                         "fingerprint": "megadetector-v6"},
            "classify": {"done": classify_done, "stale": classify_stale, "eligible_total": eligible, "total": eligible,
                         "fingerprint": f"{cur_model}|{cur_labels_fp}" if cur_model else None},
            "eye_kp":   {"done": eye_done, "stale": eye_stale, "eligible_total": eligible, "total": eligible,
                         "fingerprint": EYE_KP_FINGERPRINT_VERSION},
            "group":    {"computed": group_computed, "stale": group_stale,
                         "last_at": last_at, "fingerprint": cur_group_fp},
        }
```

**Step 4: Run passing.** All 4 tests PASS.

**Step 5: Commit**

```bash
git add vireo/db.py vireo/tests/test_db.py
git commit -m "db: add pipeline_stage_counts() helper for per-stage UI"
```

---

## Task 3.2: Wire `pipeline_stage_counts()` into `/api/pipeline/page-init`

**Files:**
- Modify: `vireo/app.py` line 1199-1213 (the page-init `jsonify` block)
- Test: `vireo/tests/test_app.py`

**Step 1: Write the failing test**

Add to `vireo/tests/test_app.py`:

```python
def test_pipeline_page_init_includes_stages_block(app_and_db):
    """page-init returns the new `stages` dict with per-stage counts."""
    app, db = app_and_db
    client = app.test_client()
    resp = client.get('/api/pipeline/page-init')
    assert resp.status_code == 200
    data = resp.get_json()
    assert "stages" in data
    stages = data["stages"]
    for key in ["scan", "previews", "detect", "classify", "eye_kp", "group"]:
        assert key in stages, f"missing stage {key}"
    assert "extract" not in stages  # deferred to Phase 3
    # backwards compat — old fields still present
    assert "has_detections" in data
    assert "has_masks" in data
    assert "has_sharpness" in data
```

**Step 2: Run failing.** Expected: FAIL — `assert "stages" in data`.

**Step 3: Implement**

In `vireo/app.py` line 1199, change the `jsonify` block to include `"stages": db.pipeline_stage_counts()`:

```python
        return jsonify({
            "total_photos": total_photos,
            "has_detections": pipeline_counts["detections"],
            "has_masks": pipeline_counts["masks"],
            "has_sharpness": pipeline_counts["sharpness"],
            "stages": db.pipeline_stage_counts(),
            "taxonomy_available": taxonomy_available,
            "pipeline_config": {
                ...
            },
            ...
        })
```

**Step 4: Run passing.** PASS.

**Step 5: Commit**

```bash
git add vireo/app.py vireo/tests/test_app.py
git commit -m "api: surface stages block in /api/pipeline/page-init"
```

---

## Task 4: Full-suite verification

**Step 1: Run the project test command**

```bash
python -m pytest tests/test_workspaces.py vireo/tests/test_db.py vireo/tests/test_app.py vireo/tests/test_photos_api.py vireo/tests/test_edits_api.py vireo/tests/test_jobs_api.py vireo/tests/test_darktable_api.py vireo/tests/test_config.py vireo/tests/test_pipeline.py vireo/tests/test_pipeline_job.py -v
```

**Step 2: Triage results.** Two known failures from `MEMORY.md` are pre-existing and out of scope:
- `test_remove_keyword_from_photo`
- `test_undo_keyword_remove_clears_pending_change`

Anything else that fails is on us — fix before opening the PR.

**Step 3: Open the PR**

```bash
git push -u origin pipeline-rerun-check
gh pr create --base main --title "pipeline: backend stage counts (Phase 1 of status makeover)" --body "$(cat <<'EOF'
## Summary
Phase 1 of the pipeline status makeover ([design doc](docs/plans/2026-05-01-pipeline-status-makeover-design.md)). Backend-only. No UI changes.

- New columns: `photos.eye_kp_fingerprint`, `workspaces.last_grouped_at`, `workspaces.last_group_fingerprint`. Backfilled to current values so existing data isn't flagged "Outdated" on first upgrade.
- New constants: `EYE_KP_FINGERPRINT_VERSION`, `compute_group_fingerprint()`.
- Pipeline now stamps these fingerprints when the stages complete.
- New `db.pipeline_stage_counts()` returns the `stages` block (scan / previews / detect / classify / eye_kp / group). Extract is intentionally deferred to Phase 3 (depends on PR #736).
- `/api/pipeline/page-init` exposes `stages` alongside the existing `has_*` fields (kept during transition).

## Test plan
- [x] All new tests passing (test_db, test_pipeline, test_app).
- [x] Full focused suite green minus the two pre-existing failures noted in MEMORY.md.
- [x] Manual: page loads unchanged (no UI consumes `stages` yet).
EOF
)"
```

---

## Out of scope (Phase 2+)

- UI consumption of `stages` (Phase 2)
- Per-variant Extract block (Phase 3 — depends on PR #736)
- SSE live count updates during run (Phase 4)
- Removing the legacy `has_detections` / `has_masks` / `has_sharpness` fields (Phase 4)
