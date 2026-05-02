# SAM Mask History Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Allow Vireo to keep multiple SAM masks per photo (one per SAM variant), let the user compare them in the lightbox and pick which one drives downstream scoring, and surface per-variant storage management on the stats page.

**Architecture:** A new `photo_masks` table holds `(photo_id, variant) → mask path + prompt provenance + per-mask features`, with PK `(photo_id, variant)`. Mask files become `~/.vireo/masks/{photo_id}.{variant}.png`. A new `photos.active_mask_variant` column points at the row whose values are denormalized into the existing `photos.mask_path / subject_tenengrad / bg_tenengrad / crop_complete` columns, so all downstream readers (scoring, pipeline.py) keep working unchanged. The masking job skips re-running when a row already exists for `(photo_id, variant)` AND its stored detection prompt still matches the photo's current primary detection. Existing masks migrate to `variant = 'unknown'` and get regenerated on the next pipeline run.

**Tech Stack:** SQLite (WAL), Flask + Jinja2, vanilla JS, pytest. ONNX Runtime for SAM2 (existing).

---

## Background context (for engineers with no Vireo familiarity)

- `vireo/db.py` — `Database` class. The `photos` table schema is at line 223. Migrations follow the pattern at line 632 (`SELECT col FROM photos LIMIT 0` in a try/except with `ALTER TABLE photos ADD COLUMN ...` on `OperationalError`).
- `vireo/pipeline_job.py:2310-2510` — the `extract_masks` stage of the pipeline job. Calls `generate_mask` and `save_mask` from `vireo/masking.py`, plus `update_photo_pipeline_features` and `update_photo_embeddings` on the DB.
- `vireo/masking.py:333-349` — `save_mask` writes `~/.vireo/masks/{photo_id}.png`. The directory is `os.path.join(os.path.dirname(db_path), "masks")`.
- `vireo/pipeline.py:24-27, 273-280` — pipeline state SELECTs read `mask_path`, `subject_tenengrad`, `bg_tenengrad`, `crop_complete` directly from the `photos` table.
- `vireo/scoring.py` — reads `mask_path` to load per-photo masks during scoring/selection. Don't refactor; we keep `photos.mask_path` populated.
- `vireo/templates/stats.html:220-251` — Storage section in the stats page. Existing cards: thumbnails, previews, embeddings.
- `vireo/templates/_navbar.html` — contains the lightbox shared across pages.
- `vireo/templates/pipeline.html` — pipeline configuration UI; `sam2_variant` dropdown is around line 3055-3093.
- Detections schema: `vireo/db.py:348-359`. YOLO re-runs do `DELETE FROM detections WHERE photo_id=? AND detector_model=?` then re-INSERT (so detection IDs are not stable across re-runs).
- The masking job picks `dets[0]` (highest confidence after `category != 'full-image'` filter) as the SAM prompt — see `vireo/pipeline_job.py:2347`.

**Test command (run after every implementation step that changes Python):**

```bash
python -m pytest tests/test_workspaces.py vireo/tests/test_db.py vireo/tests/test_app.py vireo/tests/test_photos_api.py vireo/tests/test_edits_api.py vireo/tests/test_jobs_api.py vireo/tests/test_darktable_api.py vireo/tests/test_config.py vireo/tests/test_masking.py vireo/tests/test_pipeline.py vireo/tests/test_pipeline_api.py -v
```

**Plan-doc convention:** `docs/plans/` is gitignored. Force-add design+plan files with `git add -f docs/plans/...md` per project convention.

**Workflow reminder:** All work happens on a feature branch. The branch for this plan is `sam-models-explain` (already created). Push changes to that branch and open a PR against `main` when complete.

---

## Phase 1 — Schema + DB methods

### Task 1.1: Create `photo_masks` table on init

**Files:**
- Modify: `vireo/db.py` (insert next to existing `CREATE TABLE` blocks, near line 360 after `detections`)
- Test: `vireo/tests/test_db.py`

**Step 1: Write the failing test**

Append to `vireo/tests/test_db.py`:

```python
def test_photo_masks_table_exists(tmp_path):
    """photo_masks table must exist on a fresh DB."""
    from db import Database
    db = Database(str(tmp_path / "v.db"))
    cols = {row[1] for row in db.conn.execute(
        "PRAGMA table_info(photo_masks)"
    ).fetchall()}
    assert {
        "photo_id", "variant", "path", "created_at",
        "detector_model", "prompt_x", "prompt_y", "prompt_w", "prompt_h",
        "subject_size", "subject_tenengrad", "bg_tenengrad", "crop_complete",
    } <= cols


def test_photo_masks_pk_is_photo_and_variant(tmp_path):
    """(photo_id, variant) is the primary key — same variant twice fails."""
    import sqlite3
    from db import Database
    db = Database(str(tmp_path / "v.db"))
    db.conn.execute("INSERT INTO folders(path) VALUES ('/tmp')")
    db.conn.execute(
        "INSERT INTO photos(id, folder_id, filename) VALUES (1, 1, 'a.jpg')"
    )
    db.conn.execute(
        "INSERT INTO photo_masks(photo_id, variant, path, created_at, "
        "detector_model, prompt_x, prompt_y, prompt_w, prompt_h) "
        "VALUES (1, 'sam2-small', '/p1', 0, 'unknown', -1, -1, -1, -1)"
    )
    with pytest.raises(sqlite3.IntegrityError):
        db.conn.execute(
            "INSERT INTO photo_masks(photo_id, variant, path, created_at, "
            "detector_model, prompt_x, prompt_y, prompt_w, prompt_h) "
            "VALUES (1, 'sam2-small', '/p2', 1, 'unknown', -1, -1, -1, -1)"
        )
```

**Step 2: Run failing**

```
python -m pytest vireo/tests/test_db.py::test_photo_masks_table_exists vireo/tests/test_db.py::test_photo_masks_pk_is_photo_and_variant -v
```

Expected: FAIL — `no such table: photo_masks`.

**Step 3: Implement**

In `vireo/db.py`, after the `CREATE TABLE IF NOT EXISTS detections (...)` block (around line 359), add:

```python
            CREATE TABLE IF NOT EXISTS photo_masks (
                photo_id          INTEGER NOT NULL REFERENCES photos(id) ON DELETE CASCADE,
                variant           TEXT    NOT NULL,
                path              TEXT    NOT NULL,
                created_at        INTEGER NOT NULL,
                detector_model    TEXT    NOT NULL,
                prompt_x          INTEGER NOT NULL,
                prompt_y          INTEGER NOT NULL,
                prompt_w          INTEGER NOT NULL,
                prompt_h          INTEGER NOT NULL,
                subject_size      INTEGER,
                subject_tenengrad REAL,
                bg_tenengrad      REAL,
                crop_complete     REAL,
                PRIMARY KEY (photo_id, variant)
            );
```

**Step 4: Run passing**

Same command. Expected: PASS.

**Step 5: Commit**

```bash
git add vireo/db.py vireo/tests/test_db.py
git commit -m "db: add photo_masks table for per-variant SAM masks"
```

---

### Task 1.2: Add `active_mask_variant` column to `photos`

**Files:**
- Modify: `vireo/db.py` (the migration block around line 632)
- Test: `vireo/tests/test_db.py`

**Step 1: Test first**

```python
def test_photos_has_active_mask_variant_column(tmp_path):
    from db import Database
    db = Database(str(tmp_path / "v.db"))
    db.conn.execute("SELECT active_mask_variant FROM photos LIMIT 0")
```

**Step 2: Run failing.** Expected: PASS or FAIL depending on whether column exists. It does not yet — FAIL with `no such column`.

**Step 3: Implement**

In `vireo/db.py`, in the migration section that includes `working_copy_failed_at` (around line 631), add:

```python
        try:
            self.conn.execute("SELECT active_mask_variant FROM photos LIMIT 0")
        except sqlite3.OperationalError:
            self.conn.execute(
                "ALTER TABLE photos ADD COLUMN active_mask_variant TEXT"
            )
```

Also add `active_mask_variant TEXT` in the `CREATE TABLE photos (...)` block at line 223 right after `dino_embedding_variant TEXT,` so fresh DBs have the column natively.

**Step 4: Run passing.** Same command, PASS.

**Step 5: Commit**

```bash
git add vireo/db.py vireo/tests/test_db.py
git commit -m "db: add photos.active_mask_variant column"
```

---

### Task 1.3: `upsert_photo_mask`

**Files:**
- Modify: `vireo/db.py` (add new method near `update_photo_mask` at line 3582)
- Test: `vireo/tests/test_db.py`

**Step 1: Test**

```python
def test_upsert_photo_mask_inserts_and_replaces(tmp_path):
    from db import Database
    db = Database(str(tmp_path / "v.db"))
    db.conn.execute("INSERT INTO folders(path) VALUES ('/tmp')")
    db.conn.execute(
        "INSERT INTO photos(id, folder_id, filename) VALUES (1, 1, 'a.jpg')"
    )

    db.upsert_photo_mask(
        photo_id=1, variant="sam2-small", path="/m/1.sam2-small.png",
        detector_model="megadetector-v6",
        prompt_x=10, prompt_y=20, prompt_w=100, prompt_h=200,
        subject_size=20000, subject_tenengrad=1.5,
        bg_tenengrad=0.3, crop_complete=1.0,
    )
    row = db.conn.execute(
        "SELECT path, prompt_x FROM photo_masks WHERE photo_id=1 AND variant='sam2-small'"
    ).fetchone()
    assert row["path"] == "/m/1.sam2-small.png"
    assert row["prompt_x"] == 10

    # Re-upsert with new prompt — row replaced
    db.upsert_photo_mask(
        photo_id=1, variant="sam2-small", path="/m/1.sam2-small.png",
        detector_model="megadetector-v6",
        prompt_x=11, prompt_y=20, prompt_w=100, prompt_h=200,
        subject_size=21000, subject_tenengrad=1.5,
        bg_tenengrad=0.3, crop_complete=1.0,
    )
    row = db.conn.execute(
        "SELECT prompt_x, subject_size FROM photo_masks WHERE photo_id=1 AND variant='sam2-small'"
    ).fetchone()
    assert row["prompt_x"] == 11
    assert row["subject_size"] == 21000
    # Still exactly one row for this (photo, variant)
    n = db.conn.execute(
        "SELECT COUNT(*) FROM photo_masks WHERE photo_id=1 AND variant='sam2-small'"
    ).fetchone()[0]
    assert n == 1
```

**Step 2: Fail.** PASS-of-fail: PASS-test fails with `AttributeError: 'Database' object has no attribute 'upsert_photo_mask'`.

**Step 3: Implement** — add to `vireo/db.py` after `update_photo_mask`:

```python
    def upsert_photo_mask(
        self, photo_id, variant, path,
        detector_model, prompt_x, prompt_y, prompt_w, prompt_h,
        subject_size=None, subject_tenengrad=None,
        bg_tenengrad=None, crop_complete=None,
    ):
        """Insert or replace a mask row for (photo_id, variant)."""
        import time
        self.conn.execute(
            """
            INSERT INTO photo_masks (
                photo_id, variant, path, created_at,
                detector_model, prompt_x, prompt_y, prompt_w, prompt_h,
                subject_size, subject_tenengrad, bg_tenengrad, crop_complete
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(photo_id, variant) DO UPDATE SET
                path=excluded.path,
                created_at=excluded.created_at,
                detector_model=excluded.detector_model,
                prompt_x=excluded.prompt_x,
                prompt_y=excluded.prompt_y,
                prompt_w=excluded.prompt_w,
                prompt_h=excluded.prompt_h,
                subject_size=excluded.subject_size,
                subject_tenengrad=excluded.subject_tenengrad,
                bg_tenengrad=excluded.bg_tenengrad,
                crop_complete=excluded.crop_complete
            """,
            (photo_id, variant, path, int(time.time()),
             detector_model, prompt_x, prompt_y, prompt_w, prompt_h,
             subject_size, subject_tenengrad, bg_tenengrad, crop_complete),
        )
        commit_with_retry(self.conn)
```

**Step 4: Pass.**

**Step 5: Commit**

```bash
git add vireo/db.py vireo/tests/test_db.py
git commit -m "db: add upsert_photo_mask"
```

---

### Task 1.4: `get_photo_mask` and `list_masks_for_photo`

**Step 1: Test**

```python
def test_get_photo_mask_returns_row_or_none(tmp_path):
    from db import Database
    db = Database(str(tmp_path / "v.db"))
    db.conn.execute("INSERT INTO folders(path) VALUES ('/tmp')")
    db.conn.execute("INSERT INTO photos(id, folder_id, filename) VALUES (1, 1, 'a.jpg')")
    assert db.get_photo_mask(1, "sam2-small") is None

    db.upsert_photo_mask(
        photo_id=1, variant="sam2-small", path="/p", detector_model="md",
        prompt_x=1, prompt_y=2, prompt_w=3, prompt_h=4,
    )
    m = db.get_photo_mask(1, "sam2-small")
    assert m["path"] == "/p"
    assert m["detector_model"] == "md"
    assert m["prompt_x"] == 1


def test_list_masks_for_photo(tmp_path):
    from db import Database
    db = Database(str(tmp_path / "v.db"))
    db.conn.execute("INSERT INTO folders(path) VALUES ('/tmp')")
    db.conn.execute("INSERT INTO photos(id, folder_id, filename) VALUES (1, 1, 'a.jpg')")
    db.upsert_photo_mask(photo_id=1, variant="sam2-small", path="/a",
        detector_model="md", prompt_x=1, prompt_y=2, prompt_w=3, prompt_h=4)
    db.upsert_photo_mask(photo_id=1, variant="sam2-large", path="/b",
        detector_model="md", prompt_x=1, prompt_y=2, prompt_w=3, prompt_h=4)
    variants = sorted(m["variant"] for m in db.list_masks_for_photo(1))
    assert variants == ["sam2-large", "sam2-small"]
```

**Step 2: Fail.**

**Step 3: Implement** in `vireo/db.py`:

```python
    def get_photo_mask(self, photo_id, variant):
        row = self.conn.execute(
            "SELECT * FROM photo_masks WHERE photo_id=? AND variant=?",
            (photo_id, variant),
        ).fetchone()
        return dict(row) if row else None

    def list_masks_for_photo(self, photo_id):
        rows = self.conn.execute(
            "SELECT * FROM photo_masks WHERE photo_id=? ORDER BY created_at DESC",
            (photo_id,),
        ).fetchall()
        return [dict(r) for r in rows]
```

**Step 4: Pass. Step 5: Commit.**

```bash
git add vireo/db.py vireo/tests/test_db.py
git commit -m "db: add get_photo_mask and list_masks_for_photo"
```

---

### Task 1.5: `set_active_mask_variant` (denormalizes into photos)

This is the centerpiece for keeping downstream readers happy: when activation changes, copy that variant's path + features into the `photos` row.

**Step 1: Test**

```python
def test_set_active_mask_variant_denormalizes(tmp_path):
    from db import Database
    db = Database(str(tmp_path / "v.db"))
    db.conn.execute("INSERT INTO folders(path) VALUES ('/tmp')")
    db.conn.execute("INSERT INTO photos(id, folder_id, filename) VALUES (1, 1, 'a.jpg')")
    db.upsert_photo_mask(
        photo_id=1, variant="sam2-large", path="/m/1.sam2-large.png",
        detector_model="md", prompt_x=1, prompt_y=2, prompt_w=3, prompt_h=4,
        subject_size=12345, subject_tenengrad=2.0,
        bg_tenengrad=0.5, crop_complete=0.9,
    )
    db.set_active_mask_variant(1, "sam2-large")
    row = db.conn.execute(
        "SELECT mask_path, active_mask_variant, subject_size, "
        "subject_tenengrad, bg_tenengrad, crop_complete FROM photos WHERE id=1"
    ).fetchone()
    assert row["mask_path"] == "/m/1.sam2-large.png"
    assert row["active_mask_variant"] == "sam2-large"
    assert row["subject_size"] == 12345
    assert row["subject_tenengrad"] == 2.0


def test_set_active_mask_variant_missing_row_raises(tmp_path):
    from db import Database
    db = Database(str(tmp_path / "v.db"))
    db.conn.execute("INSERT INTO folders(path) VALUES ('/tmp')")
    db.conn.execute("INSERT INTO photos(id, folder_id, filename) VALUES (1, 1, 'a.jpg')")
    with pytest.raises(ValueError):
        db.set_active_mask_variant(1, "sam3-small")
```

**Step 2: Fail.**

**Step 3: Implement**

```python
    def set_active_mask_variant(self, photo_id, variant):
        """Mark `variant` as active for `photo_id` and denormalize its
        fields into the photos row (mask_path + per-mask features) so
        downstream readers (scoring, pipeline) see the active mask."""
        row = self.conn.execute(
            "SELECT path, subject_size, subject_tenengrad, bg_tenengrad, "
            "crop_complete FROM photo_masks WHERE photo_id=? AND variant=?",
            (photo_id, variant),
        ).fetchone()
        if row is None:
            raise ValueError(
                f"No photo_masks row for photo {photo_id} variant {variant!r}"
            )
        self.conn.execute(
            "UPDATE photos SET mask_path=?, active_mask_variant=?, "
            "subject_size=?, subject_tenengrad=?, bg_tenengrad=?, "
            "crop_complete=? WHERE id=?",
            (row["path"], variant, row["subject_size"],
             row["subject_tenengrad"], row["bg_tenengrad"],
             row["crop_complete"], photo_id),
        )
        commit_with_retry(self.conn)
```

**Step 4: Pass. Step 5: Commit.**

---

### Task 1.6: `delete_masks_for_variant`, `delete_inactive_masks`, `find_stale_masks`

These power the storage dashboard. Each gets its own test + implementation.

**Step 1: Tests**

```python
def test_delete_masks_for_variant_removes_files_and_rows(tmp_path):
    from db import Database
    db = Database(str(tmp_path / "v.db"))
    db.conn.execute("INSERT INTO folders(path) VALUES ('/tmp')")
    db.conn.execute("INSERT INTO photos(id, folder_id, filename) VALUES (1, 1, 'a.jpg')")
    db.conn.execute("INSERT INTO photos(id, folder_id, filename) VALUES (2, 1, 'b.jpg')")

    masks_dir = tmp_path / "masks"
    masks_dir.mkdir()
    p1 = masks_dir / "1.sam2-small.png"; p1.write_bytes(b"x")
    p2 = masks_dir / "2.sam2-small.png"; p2.write_bytes(b"y")
    db.upsert_photo_mask(1, "sam2-small", str(p1),
        detector_model="md", prompt_x=0, prompt_y=0, prompt_w=0, prompt_h=0)
    db.upsert_photo_mask(2, "sam2-small", str(p2),
        detector_model="md", prompt_x=0, prompt_y=0, prompt_w=0, prompt_h=0)

    deleted = db.delete_masks_for_variant("sam2-small")
    assert deleted == 2
    assert not p1.exists() and not p2.exists()
    assert db.conn.execute(
        "SELECT COUNT(*) FROM photo_masks WHERE variant='sam2-small'"
    ).fetchone()[0] == 0


def test_delete_masks_for_variant_refuses_active(tmp_path):
    from db import Database
    db = Database(str(tmp_path / "v.db"))
    db.conn.execute("INSERT INTO folders(path) VALUES ('/tmp')")
    db.conn.execute("INSERT INTO photos(id, folder_id, filename) VALUES (1, 1, 'a.jpg')")
    masks_dir = tmp_path / "masks"; masks_dir.mkdir()
    p = masks_dir / "1.sam2-small.png"; p.write_bytes(b"x")
    db.upsert_photo_mask(1, "sam2-small", str(p),
        detector_model="md", prompt_x=0, prompt_y=0, prompt_w=0, prompt_h=0)
    db.set_active_mask_variant(1, "sam2-small")
    with pytest.raises(ValueError):
        db.delete_masks_for_variant("sam2-small")


def test_find_stale_masks(tmp_path):
    from db import Database
    db = Database(str(tmp_path / "v.db"))
    db.conn.execute("INSERT INTO folders(path) VALUES ('/tmp')")
    db.conn.execute("INSERT INTO photos(id, folder_id, filename) VALUES (1, 1, 'a.jpg')")
    db.conn.execute(
        "INSERT INTO detections(photo_id, detector_model, box_x, box_y, "
        "box_w, box_h, detector_confidence, category) "
        "VALUES (1, 'megadetector-v6', 10, 20, 100, 200, 0.9, 'animal')"
    )
    # Mask was made from the same prompt → not stale
    db.upsert_photo_mask(1, "sam2-small", "/p",
        detector_model="megadetector-v6",
        prompt_x=10, prompt_y=20, prompt_w=100, prompt_h=200)
    assert db.find_stale_masks() == []

    # Insert a mask whose prompt no longer matches the current detection
    db.upsert_photo_mask(1, "sam2-large", "/q",
        detector_model="megadetector-v6",
        prompt_x=99, prompt_y=20, prompt_w=100, prompt_h=200)
    stale = db.find_stale_masks()
    assert {(s["photo_id"], s["variant"]) for s in stale} == {(1, "sam2-large")}
```

**Step 2: Fail.**

**Step 3: Implement** in `vireo/db.py`:

```python
    def delete_masks_for_variant(self, variant):
        """Delete all photo_masks rows + files for a variant.
        Refuses if the variant is active for any photo (caller must
        switch active first)."""
        active_count = self.conn.execute(
            "SELECT COUNT(*) FROM photos WHERE active_mask_variant=?",
            (variant,),
        ).fetchone()[0]
        if active_count > 0:
            raise ValueError(
                f"Variant {variant!r} is active for {active_count} photo(s); "
                "switch active variant before deleting"
            )
        rows = self.conn.execute(
            "SELECT path FROM photo_masks WHERE variant=?", (variant,),
        ).fetchall()
        for r in rows:
            try:
                if r["path"] and os.path.isfile(r["path"]):
                    os.remove(r["path"])
            except OSError:
                log.warning("Failed to remove mask file %s", r["path"])
        self.conn.execute("DELETE FROM photo_masks WHERE variant=?", (variant,))
        commit_with_retry(self.conn)
        return len(rows)

    def delete_inactive_masks(self):
        """Delete all photo_masks rows + files except the active variant
        per photo. Returns the number of rows deleted."""
        rows = self.conn.execute(
            "SELECT pm.photo_id, pm.variant, pm.path FROM photo_masks pm "
            "JOIN photos p ON p.id = pm.photo_id "
            "WHERE p.active_mask_variant IS NULL "
            "   OR p.active_mask_variant != pm.variant"
        ).fetchall()
        for r in rows:
            try:
                if r["path"] and os.path.isfile(r["path"]):
                    os.remove(r["path"])
            except OSError:
                log.warning("Failed to remove mask file %s", r["path"])
            self.conn.execute(
                "DELETE FROM photo_masks WHERE photo_id=? AND variant=?",
                (r["photo_id"], r["variant"]),
            )
        commit_with_retry(self.conn)
        return len(rows)

    def find_stale_masks(self):
        """Return photo_masks rows whose stored prompt no longer matches
        the photo's current primary detection (highest-confidence,
        non full-image)."""
        rows = self.conn.execute(
            """
            SELECT pm.photo_id, pm.variant, pm.path,
                   pm.detector_model, pm.prompt_x, pm.prompt_y,
                   pm.prompt_w, pm.prompt_h
              FROM photo_masks pm
             WHERE NOT EXISTS (
                SELECT 1 FROM detections d
                 WHERE d.photo_id = pm.photo_id
                   AND d.detector_model = pm.detector_model
                   AND d.detector_model != 'full-image'
                   AND CAST(d.box_x AS INTEGER) = pm.prompt_x
                   AND CAST(d.box_y AS INTEGER) = pm.prompt_y
                   AND CAST(d.box_w AS INTEGER) = pm.prompt_w
                   AND CAST(d.box_h AS INTEGER) = pm.prompt_h
             )
            """
        ).fetchall()
        return [dict(r) for r in rows]

    def delete_stale_masks(self):
        """Remove rows + files for masks whose prompt no longer matches
        the current primary detection. Skips active variants (caller can
        re-run them through the pipeline instead of dropping the
        currently-displayed mask)."""
        stale = self.find_stale_masks()
        deleted = 0
        for s in stale:
            is_active = self.conn.execute(
                "SELECT 1 FROM photos WHERE id=? AND active_mask_variant=?",
                (s["photo_id"], s["variant"]),
            ).fetchone()
            if is_active:
                continue
            try:
                if s["path"] and os.path.isfile(s["path"]):
                    os.remove(s["path"])
            except OSError:
                log.warning("Failed to remove mask file %s", s["path"])
            self.conn.execute(
                "DELETE FROM photo_masks WHERE photo_id=? AND variant=?",
                (s["photo_id"], s["variant"]),
            )
            deleted += 1
        commit_with_retry(self.conn)
        return deleted
```

**Step 4: Pass. Step 5: Commit.**

```bash
git add vireo/db.py vireo/tests/test_db.py
git commit -m "db: add mask deletion and staleness queries"
```

---

### Task 1.7: `mask_variants_summary` (per-variant counts + bytes)

**Step 1: Test**

```python
def test_mask_variants_summary(tmp_path):
    from db import Database
    db = Database(str(tmp_path / "v.db"))
    db.conn.execute("INSERT INTO folders(path) VALUES ('/tmp')")
    for pid in (1, 2, 3):
        db.conn.execute(
            "INSERT INTO photos(id, folder_id, filename) VALUES (?, 1, ?)",
            (pid, f"p{pid}.jpg"),
        )
    md = tmp_path / "masks"; md.mkdir()
    for pid, var, size in [(1, "sam2-small", 100), (2, "sam2-small", 200),
                            (1, "sam2-large", 500), (3, "sam3-small", 700)]:
        p = md / f"{pid}.{var}.png"; p.write_bytes(b"x" * size)
        db.upsert_photo_mask(pid, var, str(p),
            detector_model="md", prompt_x=0, prompt_y=0, prompt_w=0, prompt_h=0)
    db.set_active_mask_variant(1, "sam2-large")

    summary = {s["variant"]: s for s in db.mask_variants_summary()}
    assert summary["sam2-small"]["count"] == 2
    assert summary["sam2-small"]["bytes"] == 300
    assert summary["sam2-large"]["count"] == 1
    assert summary["sam2-large"]["active_count"] == 1
    assert summary["sam3-small"]["active_count"] == 0
```

**Step 2: Fail.**

**Step 3: Implement** in `vireo/db.py`:

```python
    def mask_variants_summary(self):
        """Per-variant summary: count, total bytes (best-effort, sums
        on-disk file sizes), and active_count.

        Returns: list of dicts ordered by variant name.
        """
        rows = self.conn.execute(
            """
            SELECT pm.variant,
                   COUNT(*) AS count,
                   SUM(CASE WHEN p.active_mask_variant = pm.variant
                            THEN 1 ELSE 0 END) AS active_count
              FROM photo_masks pm
              JOIN photos p ON p.id = pm.photo_id
             GROUP BY pm.variant
             ORDER BY pm.variant
            """
        ).fetchall()
        out = []
        for r in rows:
            paths = self.conn.execute(
                "SELECT path FROM photo_masks WHERE variant=?", (r["variant"],),
            ).fetchall()
            total = 0
            for pr in paths:
                try:
                    if pr["path"] and os.path.isfile(pr["path"]):
                        total += os.path.getsize(pr["path"])
                except OSError:
                    pass
            out.append({
                "variant": r["variant"],
                "count": r["count"],
                "active_count": r["active_count"],
                "bytes": total,
            })
        return out
```

**Step 4: Pass. Step 5: Commit.**

---

## Phase 2 — Filename + masking job integration

### Task 2.1: `save_mask` writes per-variant filename

**Files:**
- Modify: `vireo/masking.py:333-349`
- Modify: `vireo/masking.py:352-366` (load_mask)
- Test: `vireo/tests/test_masking.py`

**Step 1: Test**

```python
def test_save_mask_uses_variant_in_filename(tmp_path):
    import numpy as np
    from masking import save_mask
    mask = np.array([[True, False], [False, True]], dtype=bool)
    out = save_mask(mask, str(tmp_path), photo_id=42, variant="sam2-large")
    assert out == str(tmp_path / "42.sam2-large.png")
    assert (tmp_path / "42.sam2-large.png").exists()
```

**Step 2: Fail.** `save_mask()` does not yet accept a `variant` arg.

**Step 3: Implement.** Change `save_mask` signature:

```python
def save_mask(mask, masks_dir, photo_id, variant):
    """Save a boolean mask as a single-channel PNG.

    Filename: ``{photo_id}.{variant}.png`` so multiple variants per photo
    coexist on disk.
    """
    os.makedirs(masks_dir, exist_ok=True)
    path = os.path.join(masks_dir, f"{photo_id}.{variant}.png")
    mask_img = Image.fromarray((mask.astype(np.uint8) * 255), mode="L")
    mask_img.save(path, format="PNG")
    return path
```

Update `load_mask` to require a `variant` argument too:

```python
def load_mask(masks_dir, photo_id, variant):
    path = os.path.join(masks_dir, f"{photo_id}.{variant}.png")
    if not os.path.exists(path):
        return None
    with Image.open(path) as mask_img:
        return np.array(mask_img.convert("L")) > 127
```

Grep for all call sites of `save_mask` and `load_mask` and add the `variant` arg:

```bash
grep -rn "save_mask\|load_mask" vireo/ tests/
```

There is one production caller of `save_mask` (`vireo/pipeline_job.py:2480`); that gets touched in Task 2.2. Tests in `vireo/tests/test_masking.py` need updating too.

**Step 4: Pass.**

**Step 5: Commit.**

```bash
git add vireo/masking.py vireo/tests/test_masking.py
git commit -m "masking: include variant in mask filename"
```

---

### Task 2.2: Masking job uses `photo_masks`, skips when prompt unchanged

**Files:**
- Modify: `vireo/pipeline_job.py:2310-2510`
- Test: `vireo/tests/test_pipeline.py` and/or `vireo/tests/test_pipeline_api.py` (whichever covers the masking stage)

**Step 1: Tests** — three cases.

```python
def test_masking_job_skips_when_cached_with_same_prompt(...):
    """If photo_masks already has a row for (photo, variant) with a
    matching prompt, generate_mask is not called and the row is left
    alone."""
    # Use unittest.mock.patch on masking.generate_mask; assert not called.

def test_masking_job_runs_when_variant_differs(...):
    """A row exists for sam2-small; running with sam2-large produces a
    new row alongside it."""

def test_masking_job_runs_when_prompt_changed(...):
    """Existing row's prompt differs from current primary detection's
    bbox → generate_mask is called and the row is replaced."""
```

(Use the existing pipeline-job test fixtures; copy patterns from `test_pipeline.py`.)

**Step 2: Fail.**

**Step 3: Implement.** Replace the inner mask loop body in `vireo/pipeline_job.py:2453-2509`. Logical changes:

1. Determine the current primary detection's prompt for each photo (already done in the build of `photo_det_map`; persist `detector_model` and the bbox into the entry).
2. Before calling `generate_mask`, look up `thread_db.get_photo_mask(photo_id, sam2_variant)`. If it exists and `(detector_model, prompt_x/y/w/h)` matches, skip.
3. After `generate_mask`+`save_mask`, call `thread_db.upsert_photo_mask(...)` with all per-mask features and prompt fields.
4. Then call `thread_db.set_active_mask_variant(photo_id, sam2_variant)` to denormalize into the photos row.
5. Drop the call to `update_photo_pipeline_features(mask_path=..., crop_complete=..., **features)` since `set_active_mask_variant` now handles that. Embeddings call (`update_photo_embeddings`) stays as-is.
6. Replace the existing "has_mask" precheck (line 2348-2351) with a check that doesn't short-circuit when the configured variant differs from what's already cached. Specifically: include all photos with detections; the per-photo skip happens inside the loop (step 2 above) so changing the configured variant naturally re-runs.

Sketch:

```python
# Build photo_det_map (replace the existing block)
photo_det_map = {}
photos_with_detections = 0
for p in photos:
    dets = [d for d in thread_db.get_detections(p["id"])
            if d["detector_model"] != "full-image"]
    if dets:
        photos_with_detections += 1
        primary = dets[0]
        photo_det_map[p["id"]] = {
            "photo": p,
            "det_box": {
                "x": primary["box_x"], "y": primary["box_y"],
                "w": primary["box_w"], "h": primary["box_h"],
            },
            "detector_model": primary["detector_model"],
            "prompt": (
                int(primary["box_x"]), int(primary["box_y"]),
                int(primary["box_w"]), int(primary["box_h"]),
            ),
        }

# Inside the loop, before generate_mask:
existing = thread_db.get_photo_mask(photo_id, sam2_variant)
if existing:
    cached_prompt = (
        existing["prompt_x"], existing["prompt_y"],
        existing["prompt_w"], existing["prompt_h"],
    )
    if (existing["detector_model"] == entry["detector_model"]
            and cached_prompt == entry["prompt"]
            and os.path.isfile(existing["path"])):
        # Cached + prompt unchanged → skip SAM, just (re-)activate.
        thread_db.set_active_mask_variant(photo_id, sam2_variant)
        masked += 1
        processed = i + 1
        continue

# ... existing generate_mask + save_mask + features computation,
# then:
mask_path = save_mask(mask, masks_dir, photo_id, sam2_variant)
completeness = crop_completeness(mask)
features = compute_all_quality_features(proxy, mask)

thread_db.upsert_photo_mask(
    photo_id=photo_id, variant=sam2_variant, path=mask_path,
    detector_model=entry["detector_model"],
    prompt_x=entry["prompt"][0], prompt_y=entry["prompt"][1],
    prompt_w=entry["prompt"][2], prompt_h=entry["prompt"][3],
    subject_size=features.get("subject_size"),
    subject_tenengrad=features.get("subject_tenengrad"),
    bg_tenengrad=features.get("bg_tenengrad"),
    crop_complete=completeness,
)
thread_db.set_active_mask_variant(photo_id, sam2_variant)

# Embeddings unchanged:
thread_db.update_photo_embeddings(...)
```

**Pitfall to avoid:** `compute_all_quality_features` returns `subject_tenengrad`, `bg_tenengrad`, `subject_clip_high`, `subject_clip_low`, `subject_y_median`, `noise_estimate`, `phash_crop`, `subject_size`, etc. — only the mask-derived ones (`subject_size`, `subject_tenengrad`, `bg_tenengrad`) move to `photo_masks`. The rest still need to land on the `photos` row via `update_photo_pipeline_features`. Keep that call but remove the mask-related kwargs from it.

**Step 4: Pass.**

**Step 5: Commit.**

```bash
git add vireo/pipeline_job.py vireo/tests/test_pipeline.py
git commit -m "masking: cache per-variant masks and skip when prompt unchanged"
```

---

### Task 2.3: `update_photo_mask` is removed (callers refactored)

`update_photo_mask` (db.py:3582) is now unused. Grep:

```bash
grep -rn "update_photo_mask\b" vireo/ tests/
```

If only tests remain, delete the method and the tests that call it. Otherwise refactor remaining callers to `upsert_photo_mask` + `set_active_mask_variant`.

**Commit:**

```bash
git add vireo/db.py vireo/tests/test_db.py
git commit -m "db: drop unused update_photo_mask"
```

---

## Phase 3 — Migration of existing data

### Task 3.1: One-time migration on DB init

**Files:**
- Modify: `vireo/db.py` (add a migration step right after the `photo_masks` table is created and after the `active_mask_variant` ALTER block runs)
- Test: `vireo/tests/test_db.py`

**Step 1: Test**

```python
def test_existing_masks_migrate_to_unknown_variant(tmp_path):
    """A photos row with mask_path set on a pre-migration DB gets a
    photo_masks row with variant='unknown' and prompt=-1."""
    import sqlite3
    db_path = tmp_path / "v.db"

    # Build a DB with the old shape, no photo_masks table.
    conn = sqlite3.connect(str(db_path))
    conn.execute("CREATE TABLE folders (id INTEGER PRIMARY KEY, path TEXT)")
    conn.execute(
        "CREATE TABLE photos (id INTEGER PRIMARY KEY, folder_id INTEGER, "
        "filename TEXT, mask_path TEXT, subject_size INTEGER, "
        "subject_tenengrad REAL, bg_tenengrad REAL, crop_complete REAL)"
    )
    conn.execute("INSERT INTO folders(id, path) VALUES (1, '/tmp')")
    conn.execute(
        "INSERT INTO photos(id, folder_id, filename, mask_path, "
        "subject_tenengrad, crop_complete) "
        "VALUES (1, 1, 'a.jpg', '/m/1.png', 1.5, 0.9)"
    )
    conn.commit()
    conn.close()

    from db import Database
    db = Database(str(db_path))

    row = db.conn.execute(
        "SELECT * FROM photo_masks WHERE photo_id=1"
    ).fetchone()
    assert row is not None
    assert row["variant"] == "unknown"
    assert row["detector_model"] == "unknown"
    assert row["prompt_x"] == -1
    assert row["path"] == "/m/1.png"
    assert row["subject_tenengrad"] == 1.5
    assert row["crop_complete"] == 0.9
    # And photos.active_mask_variant is set
    av = db.conn.execute(
        "SELECT active_mask_variant FROM photos WHERE id=1"
    ).fetchone()[0]
    assert av == "unknown"
```

**Step 2: Fail.**

**Step 3: Implement.** Add to `vireo/db.py` immediately after the `ALTER TABLE photos ADD COLUMN active_mask_variant` migration block:

```python
        # Migrate any pre-existing masks (mask_path set on photos but no
        # photo_masks row) to variant='unknown' with sentinel prompt.
        # Their prompt provenance is unknown, so the staleness check will
        # treat them as stale on the next pipeline run and they'll be
        # regenerated.
        try:
            already = self.conn.execute(
                "SELECT COUNT(*) FROM photo_masks WHERE variant='unknown'"
            ).fetchone()[0]
        except sqlite3.OperationalError:
            already = -1
        if already == 0:
            rows = self.conn.execute(
                "SELECT id, mask_path, subject_size, subject_tenengrad, "
                "bg_tenengrad, crop_complete FROM photos "
                "WHERE mask_path IS NOT NULL"
            ).fetchall()
            now = int(time.time())  # ensure `import time` at top of db.py
            for r in rows:
                self.conn.execute(
                    "INSERT OR IGNORE INTO photo_masks "
                    "(photo_id, variant, path, created_at, detector_model, "
                    "prompt_x, prompt_y, prompt_w, prompt_h, "
                    "subject_size, subject_tenengrad, bg_tenengrad, crop_complete) "
                    "VALUES (?, 'unknown', ?, ?, 'unknown', -1, -1, -1, -1, ?, ?, ?, ?)",
                    (r["id"], r["mask_path"], now,
                     r["subject_size"], r["subject_tenengrad"],
                     r["bg_tenengrad"], r["crop_complete"]),
                )
                self.conn.execute(
                    "UPDATE photos SET active_mask_variant='unknown' "
                    "WHERE id=? AND active_mask_variant IS NULL",
                    (r["id"],),
                )
            commit_with_retry(self.conn)
```

**Step 4: Pass. Step 5: Commit.**

```bash
git add vireo/db.py vireo/tests/test_db.py
git commit -m "db: migrate existing masks to photo_masks with variant='unknown'"
```

---

## Phase 4 — Storage dashboard

### Task 4.1: `/api/storage/masks` endpoint

**Files:**
- Modify: `vireo/app.py` (add new route near `api_storage` at line 4513)
- Test: `vireo/tests/test_app.py`

**Step 1: Test**

```python
def test_api_storage_masks_returns_summary(client, tmp_path, db_with_masks):
    r = client.get("/api/storage/masks")
    assert r.status_code == 200
    data = r.get_json()
    assert "variants" in data
    assert "total_bytes" in data
    assert "stale_count" in data
```

(Use the existing app-test fixture pattern — see `vireo/tests/test_app.py` for how fixtures provision a DB and client.)

**Step 2: Fail.**

**Step 3: Implement** in `vireo/app.py` near line 4585:

```python
    @app.route("/api/storage/masks")
    def api_storage_masks():
        """Per-variant SAM mask summary (counts, bytes, active counts)
        plus stale-mask count for the storage dashboard."""
        db = _get_db()
        variants = db.mask_variants_summary()
        stale = db.find_stale_masks()
        masks_dir = os.path.join(os.path.dirname(db_path), "masks")
        return jsonify({
            "variants": variants,
            "total_bytes": sum(v["bytes"] for v in variants),
            "stale_count": len(stale),
            "path": masks_dir,
        })
```

**Step 4: Pass. Step 5: Commit.**

---

### Task 4.2: `/api/storage/masks/delete-variant`, `/api/storage/masks/delete-inactive`, `/api/storage/masks/delete-stale`

**Step 1: Tests**

```python
def test_api_delete_mask_variant(client, db_with_masks):
    r = client.post("/api/storage/masks/delete-variant",
                    json={"variant": "sam2-small"})
    assert r.status_code == 200
    assert r.get_json()["deleted"] >= 1


def test_api_delete_mask_variant_refuses_active(client, db_with_active_variant):
    r = client.post("/api/storage/masks/delete-variant",
                    json={"variant": "sam2-large"})  # active
    assert r.status_code == 400
    assert "active" in r.get_json()["error"].lower()


def test_api_delete_inactive_masks(client, db_with_masks):
    r = client.post("/api/storage/masks/delete-inactive")
    assert r.status_code == 200
    assert "deleted" in r.get_json()


def test_api_delete_stale_masks(client, db_with_stale_mask):
    r = client.post("/api/storage/masks/delete-stale")
    assert r.status_code == 200
```

**Step 2: Fail.**

**Step 3: Implement** three thin wrappers around the DB methods:

```python
    @app.route("/api/storage/masks/delete-variant", methods=["POST"])
    def api_storage_masks_delete_variant():
        body = request.get_json(silent=True) or {}
        variant = body.get("variant", "")
        if not variant:
            return json_error("variant required")
        db = _get_db()
        try:
            n = db.delete_masks_for_variant(variant)
        except ValueError as e:
            return json_error(str(e), 400)
        log.info("Deleted %d masks for variant %s", n, variant)
        return jsonify({"ok": True, "deleted": n})

    @app.route("/api/storage/masks/delete-inactive", methods=["POST"])
    def api_storage_masks_delete_inactive():
        db = _get_db()
        n = db.delete_inactive_masks()
        log.info("Deleted %d inactive-variant masks", n)
        return jsonify({"ok": True, "deleted": n})

    @app.route("/api/storage/masks/delete-stale", methods=["POST"])
    def api_storage_masks_delete_stale():
        db = _get_db()
        n = db.delete_stale_masks()
        log.info("Deleted %d stale masks", n)
        return jsonify({"ok": True, "deleted": n})
```

**Step 4: Pass. Step 5: Commit.**

---

### Task 4.3: Masks card in `stats.html`

**Files:**
- Modify: `vireo/templates/stats.html:220-251`
- Manual test: load /stats, verify card shows variants + counts + bytes, buttons work, refusal modal appears for active variant.

**Step 1: UI scaffolding** — add a new card to the storage grid mirroring the embedding card. Show:

```
Masks (sam2-small: N1, sam2-large: N2 [active], sam3-small: N3, unknown: N4)
                                                              [Total bytes]
[ Manage ] [ Delete inactive ] [ Delete stale (K) ]
```

In the manage modal: list variants with `[active] [delete]` per row, where `[delete]` for the active variant is disabled with a hint "set another variant active first."

**Step 2: Wire JS** — fetch from `/api/storage/masks` on card mount, POST to the three new endpoints from buttons, refresh on success.

**Step 3: Manual verification**

```bash
python vireo/app.py --db ~/.vireo/vireo.db --port 8080
```

Open /stats. Confirm:
- Masks card appears with the right counts.
- Clicking "Delete inactive" shows correct deleted count.
- Trying to delete the active variant shows the API error.
- Stale count refreshes when YOLO is re-run with different settings.

**Step 4: Commit.**

```bash
git add vireo/templates/stats.html
git commit -m "stats: add Masks card with per-variant breakdown and deletion"
```

---

## Phase 5 — Pipeline page (UI option C)

### Task 5.1: Per-variant coverage stats in pipeline state

**Files:**
- Modify: `vireo/db.py` (extend `get_pipeline_state` or add adjacent method)
- Modify: `vireo/app.py` (pipeline state route — find via grep)
- Test: `vireo/tests/test_pipeline_api.py`

**Step 1: Test** — assert pipeline state JSON includes `mask_variant_coverage` with per-variant counts.

**Step 2-5: Implement, run tests, commit.**

```bash
git commit -m "pipeline: surface per-variant mask coverage"
```

### Task 5.2: Coverage badge + active-variant selector in pipeline.html

**Files:**
- Modify: `vireo/templates/pipeline.html` (around the SAM2 variant select at line 3055-3093)

Add directly below the SAM2 variant dropdown:

```
SAM2 mask coverage:
  sam2-small      12,400  [Set active]
  sam2-large       8,200  [active]
  sam3-small       3,200  [Set active]
  unknown          ----   [legacy]
```

"Set active" calls a new endpoint: `POST /api/pipeline/active-mask-variant` with `{variant: "..."}` that loops over all photos with a mask for that variant and calls `db.set_active_mask_variant(...)`.

(Test with the pipeline-state API + a manual UI check.)

```bash
git commit -m "pipeline: per-variant coverage and active-variant selector"
```

---

## Phase 6 — Lightbox compare (UI option A)

### Task 6.1: API to list a photo's available mask variants

**Files:**
- Modify: `vireo/app.py` — new route `/api/photos/<int:pid>/masks`
- Test: `vireo/tests/test_photos_api.py`

```python
@app.route("/api/photos/<int:pid>/masks")
def api_photo_masks(pid):
    db = _get_db()
    masks = db.list_masks_for_photo(pid)
    active = db.conn.execute(
        "SELECT active_mask_variant FROM photos WHERE id=?", (pid,)
    ).fetchone()[0]
    return jsonify({
        "photo_id": pid,
        "active": active,
        "variants": [
            {"variant": m["variant"],
             "url": f"/api/masks/{pid}/{m['variant']}.png",
             "created_at": m["created_at"]}
            for m in masks
        ],
    })
```

Plus a static-file route to serve mask PNGs (or reuse an existing file-serving route — grep for `mask` in `app.py` to see if one exists today; today `mask_path` is read by Python only, so the file is NOT exposed over HTTP yet and a new `send_from_directory` route is required).

```bash
git commit -m "api: list and serve a photo's mask variants"
```

### Task 6.2: Lightbox variant toggle

**Files:**
- Modify: `vireo/templates/_navbar.html` (lightbox)

Add a small dropdown in the lightbox controls labeled "Mask variant: [active|sam2-small|sam2-large|sam3-small]". Selecting an option:
- Fetches the variant's PNG from `/api/masks/<pid>/<variant>.png`.
- Replaces the current overlay image with no other side effects (does NOT change `active_mask_variant` — that's the pipeline page's job).
- Shows "n/a" if the photo has no mask for that variant.

Manual test in browser using Playwright per the user-first-testing memory.

```bash
git commit -m "lightbox: toggle mask overlay between SAM variants"
```

---

## Phase 7 — Final integration check

### Task 7.1: Full test sweep

```bash
python -m pytest tests/ vireo/tests/ -v
```

Expected: all SAM-related tests pass; the pre-existing-failures memo from `project_preexisting_test_failures.md` flags any unrelated flakiness.

### Task 7.2: End-to-end smoke

1. Start app: `python vireo/app.py --db ~/.vireo/vireo.db --port 8080`
2. Set `pipeline.sam2_variant = sam2-small`, run pipeline on a small folder. Confirm masks appear.
3. Set `pipeline.sam2_variant = sam2-large`, re-run. Confirm new variant gets its own files; sam2-small files still on disk; pipeline page shows two variants; lightbox toggle works.
4. Re-run a third time without changing variant. Confirm `generate_mask` is not called for already-masked photos (check `~/.vireo/vireo.log`).
5. In stats: "Delete inactive" leaves only the active variant; counts in the pipeline page match.
6. Re-run YOLO with a different `detector_confidence`. After it produces different bboxes, /stats shows non-zero stale_count; pipeline rerun regenerates the affected variants.

### Task 7.3: PR

```bash
gh pr create --base main --title "SAM mask history: per-variant masks, comparison UI, storage management" --body "<summary + test results>"
```

---

## Open follow-ups (out of scope for this plan)

- Replace the `unknown`-variant migration with a backfill from `pipeline_cfg.sam2_variant` when the user explicitly opts in via a UI button. Currently the migration is honest at the cost of one re-run.
- Add a `prompt_hash` column for prompt types beyond bbox (e.g., point clicks) if SAM ever takes other prompt forms.
- Consider a side-by-side-on-one-screen compare view (UI option B) once option A is in production and we know how often it gets used.
