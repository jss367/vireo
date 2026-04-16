# Preview Pyramid Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Refactor Vireo's image cache so every derivative (thumbnail, sized preview) is derived from a single canonical image per photo (working copy if present, else source JPEG), and add an LRU-bounded disk cache for sized previews with a user-configurable quota.

**Architecture:** One canonical path per photo, resolved via a new `get_canonical_image_path` helper. The scanner extracts a working copy for RAW (unchanged) and for JPEGs larger than the cap (new). Thumbnail generation and all preview endpoints read through the canonical helper. Sized previews are tracked in a new `preview_cache` SQLite table with `last_access_at`, enabling true LRU eviction when a configurable quota is exceeded. Existing on-disk preview files are adopted lazily into the LRU on first access; no forced migration.

**Tech Stack:** Flask, SQLite, Pillow, rawpy. Tests: pytest. All changes Python-only + one `settings.html` edit.

**Design doc:** `docs/plans/2026-04-16-preview-pyramid-design.md`

---

## Working directory and baseline

All work happens in the worktree at `.worktrees/preview-pyramid` on branch `claude/preview-pyramid`. Before starting, verify the baseline test command from `CLAUDE.md` passes:

```bash
cd .worktrees/preview-pyramid
python -m pytest tests/test_workspaces.py vireo/tests/test_db.py vireo/tests/test_app.py vireo/tests/test_photos_api.py vireo/tests/test_edits_api.py vireo/tests/test_jobs_api.py vireo/tests/test_darktable_api.py vireo/tests/test_config.py -v
```

Expected: all tests pass.

---

## Task 1: Add `preview_cache_max_mb` config key

**Files:**
- Modify: `vireo/config.py:39-42` (add to `DEFAULTS`)
- Test: `vireo/tests/test_config.py` (may already exist, add a case)

**Step 1: Write the failing test**

Add to `vireo/tests/test_config.py`:

```python
def test_preview_cache_max_mb_default():
    """Default config includes preview_cache_max_mb = 2048."""
    import config as cfg
    assert cfg.DEFAULTS["preview_cache_max_mb"] == 2048
```

**Step 2: Run test to verify it fails**

```bash
python -m pytest vireo/tests/test_config.py::test_preview_cache_max_mb_default -v
```

Expected: FAIL (key not in DEFAULTS).

**Step 3: Add the key**

In `vireo/config.py`, within the `DEFAULTS` dict, alongside the other preview/thumbnail keys (around line 42):

```python
"preview_cache_max_mb": 2048,
```

**Step 4: Run test to verify it passes**

```bash
python -m pytest vireo/tests/test_config.py::test_preview_cache_max_mb_default -v
```

Expected: PASS.

**Step 5: Commit**

```bash
git add vireo/config.py vireo/tests/test_config.py
git commit -m "feat: add preview_cache_max_mb config key"
```

---

## Task 2: Add `preview_cache` table to schema

**Files:**
- Modify: `vireo/db.py` (find the `CREATE TABLE` block in `__init__`, typically near line 145; and the schema-migration section that runs for existing DBs)
- Test: `vireo/tests/test_db.py`

**Background:** `Database.__init__` runs a block of `CREATE TABLE IF NOT EXISTS` statements for fresh DBs, and has a migration section below for adding columns/tables to existing DBs. Add the new table in both places — once in the fresh-schema block, once in the idempotent migration block (which can just run the same `CREATE TABLE IF NOT EXISTS` to handle upgrades).

**Step 1: Write the failing test**

Add to `vireo/tests/test_db.py`:

```python
def test_preview_cache_table_exists(tmp_path):
    """Database creates preview_cache table on init."""
    from db import Database
    db = Database(str(tmp_path / "test.db"))
    row = db.conn.execute(
        "SELECT name FROM sqlite_master WHERE type='table' AND name='preview_cache'"
    ).fetchone()
    assert row is not None

    # Verify schema columns
    cols = {r["name"] for r in db.conn.execute("PRAGMA table_info(preview_cache)").fetchall()}
    assert cols == {"photo_id", "size", "bytes", "last_access_at"}

    # Verify index
    idx = db.conn.execute(
        "SELECT name FROM sqlite_master WHERE type='index' AND name='preview_cache_last_access'"
    ).fetchone()
    assert idx is not None
```

**Step 2: Run test to verify it fails**

```bash
python -m pytest vireo/tests/test_db.py::test_preview_cache_table_exists -v
```

Expected: FAIL (table doesn't exist).

**Step 3: Add the schema**

Find the block in `vireo/db.py` that contains `CREATE TABLE IF NOT EXISTS photos` (around line 100-200). After the existing tables, add:

```python
self.conn.execute(
    """
    CREATE TABLE IF NOT EXISTS preview_cache (
        photo_id INTEGER NOT NULL,
        size INTEGER NOT NULL,
        bytes INTEGER NOT NULL,
        last_access_at REAL NOT NULL,
        PRIMARY KEY (photo_id, size),
        FOREIGN KEY (photo_id) REFERENCES photos(id) ON DELETE CASCADE
    )
    """
)
self.conn.execute(
    "CREATE INDEX IF NOT EXISTS preview_cache_last_access "
    "ON preview_cache(last_access_at)"
)
```

The `CREATE TABLE IF NOT EXISTS` is idempotent, so placing it in the fresh-schema section is sufficient — existing DBs will pick up the new table on next startup via the same block (it runs on every `Database.__init__`).

**Step 4: Run test to verify it passes**

```bash
python -m pytest vireo/tests/test_db.py::test_preview_cache_table_exists -v
```

Expected: PASS.

**Step 5: Commit**

```bash
git add vireo/db.py vireo/tests/test_db.py
git commit -m "feat: add preview_cache schema for LRU-tracked sized previews"
```

---

## Task 3: DB methods for `preview_cache`

**Files:**
- Modify: `vireo/db.py` (add new methods on `Database` class)
- Test: `vireo/tests/test_db.py`

**Step 1: Write the failing tests**

Add to `vireo/tests/test_db.py`:

```python
def test_preview_cache_insert_and_touch(tmp_path):
    """Insert a row, then touch updates last_access_at."""
    import time
    from db import Database
    db = Database(str(tmp_path / "test.db"))
    # Need a real photo row due to FK
    folder_id = db.add_folder("/tmp/test")
    photo_id = db.add_photo(folder_id, "a.jpg", "jpg")

    t0 = time.time()
    db.preview_cache_insert(photo_id, size=1920, bytes_=12345)

    row = db.conn.execute(
        "SELECT bytes, last_access_at FROM preview_cache WHERE photo_id=? AND size=?",
        (photo_id, 1920),
    ).fetchone()
    assert row["bytes"] == 12345
    assert row["last_access_at"] >= t0

    # Sleep a tiny bit, touch, confirm timestamp advances
    time.sleep(0.01)
    db.preview_cache_touch(photo_id, size=1920)
    row2 = db.conn.execute(
        "SELECT last_access_at FROM preview_cache WHERE photo_id=? AND size=?",
        (photo_id, 1920),
    ).fetchone()
    assert row2["last_access_at"] > row["last_access_at"]


def test_preview_cache_total_bytes(tmp_path):
    """total_bytes sums all rows."""
    from db import Database
    db = Database(str(tmp_path / "test.db"))
    folder_id = db.add_folder("/tmp/test")
    p1 = db.add_photo(folder_id, "a.jpg", "jpg")
    p2 = db.add_photo(folder_id, "b.jpg", "jpg")

    assert db.preview_cache_total_bytes() == 0
    db.preview_cache_insert(p1, 1920, 100)
    db.preview_cache_insert(p2, 2560, 200)
    assert db.preview_cache_total_bytes() == 300


def test_preview_cache_delete(tmp_path):
    """Delete removes the row."""
    from db import Database
    db = Database(str(tmp_path / "test.db"))
    folder_id = db.add_folder("/tmp/test")
    p1 = db.add_photo(folder_id, "a.jpg", "jpg")
    db.preview_cache_insert(p1, 1920, 100)
    db.preview_cache_delete(p1, 1920)
    assert db.preview_cache_total_bytes() == 0


def test_preview_cache_oldest_first(tmp_path):
    """Iterating in LRU order returns oldest first."""
    import time
    from db import Database
    db = Database(str(tmp_path / "test.db"))
    folder_id = db.add_folder("/tmp/test")
    p1 = db.add_photo(folder_id, "a.jpg", "jpg")
    p2 = db.add_photo(folder_id, "b.jpg", "jpg")

    db.preview_cache_insert(p1, 1920, 100)
    time.sleep(0.01)
    db.preview_cache_insert(p2, 1920, 200)

    rows = db.preview_cache_oldest_first()
    assert [(r["photo_id"], r["size"]) for r in rows] == [(p1, 1920), (p2, 1920)]
```

**Step 2: Run tests to verify they fail**

```bash
python -m pytest vireo/tests/test_db.py::test_preview_cache_insert_and_touch \
    vireo/tests/test_db.py::test_preview_cache_total_bytes \
    vireo/tests/test_db.py::test_preview_cache_delete \
    vireo/tests/test_db.py::test_preview_cache_oldest_first -v
```

Expected: all FAIL (methods not defined).

**Step 3: Implement the methods**

Add to the `Database` class in `vireo/db.py` (near other CRUD helpers, not inside `__init__`):

```python
def preview_cache_insert(self, photo_id, size, bytes_):
    """Insert or replace a preview_cache entry. last_access_at = now()."""
    import time
    self.conn.execute(
        "INSERT OR REPLACE INTO preview_cache "
        "(photo_id, size, bytes, last_access_at) VALUES (?, ?, ?, ?)",
        (photo_id, size, bytes_, time.time()),
    )
    self.conn.commit()

def preview_cache_touch(self, photo_id, size):
    """Update last_access_at for an existing entry. No-op if missing."""
    import time
    self.conn.execute(
        "UPDATE preview_cache SET last_access_at=? WHERE photo_id=? AND size=?",
        (time.time(), photo_id, size),
    )
    self.conn.commit()

def preview_cache_delete(self, photo_id, size):
    """Delete a preview_cache entry (caller removes the file)."""
    self.conn.execute(
        "DELETE FROM preview_cache WHERE photo_id=? AND size=?",
        (photo_id, size),
    )
    self.conn.commit()

def preview_cache_total_bytes(self):
    """Return total bytes tracked in preview_cache."""
    row = self.conn.execute(
        "SELECT COALESCE(SUM(bytes), 0) AS total FROM preview_cache"
    ).fetchone()
    return row["total"]

def preview_cache_oldest_first(self):
    """Return all rows ordered by last_access_at ascending (oldest first)."""
    return self.conn.execute(
        "SELECT photo_id, size, bytes, last_access_at FROM preview_cache "
        "ORDER BY last_access_at ASC"
    ).fetchall()

def preview_cache_get(self, photo_id, size):
    """Return the row for (photo_id, size), or None."""
    return self.conn.execute(
        "SELECT photo_id, size, bytes, last_access_at FROM preview_cache "
        "WHERE photo_id=? AND size=?",
        (photo_id, size),
    ).fetchone()
```

**Step 4: Run tests to verify they pass**

```bash
python -m pytest vireo/tests/test_db.py -v -k preview_cache
```

Expected: all PASS.

**Step 5: Commit**

```bash
git add vireo/db.py vireo/tests/test_db.py
git commit -m "feat: add Database methods for preview_cache LRU"
```

---

## Task 4: Canonical image path helper

**Files:**
- Modify: `vireo/image_loader.py` (add `get_canonical_image_path`)
- Test: `vireo/tests/test_image_loader.py`

**Background:** `image_loader.py` already has `load_working_image` which returns a PIL image. We need a path-returning cousin, because preview/thumbnail code opens the file, resizes, and re-encodes — a path is cheaper to pass around than a loaded image, and aligns with how `serve_full_photo`/`serve_photo_preview` already do things.

**Step 1: Write the failing test**

Add to `vireo/tests/test_image_loader.py`:

```python
def test_get_canonical_image_path_prefers_working_copy(tmp_path):
    """When working_copy_path is set and file exists, returns working copy path."""
    from image_loader import get_canonical_image_path

    vireo_dir = tmp_path / "vireo"
    (vireo_dir / "working").mkdir(parents=True)
    wc = vireo_dir / "working" / "42.jpg"
    wc.write_bytes(b"fake")

    photo = {"working_copy_path": "working/42.jpg", "folder_id": 1, "filename": "src.jpg"}
    folders = {1: "/some/folder"}

    result = get_canonical_image_path(photo, str(vireo_dir), folders)
    assert result == str(wc)


def test_get_canonical_image_path_falls_back_to_source(tmp_path):
    """When no working_copy_path, returns folder/filename."""
    from image_loader import get_canonical_image_path

    photo = {"working_copy_path": None, "folder_id": 1, "filename": "src.jpg"}
    folders = {1: "/some/folder"}

    result = get_canonical_image_path(photo, str(tmp_path), folders)
    assert result == "/some/folder/src.jpg"


def test_get_canonical_image_path_wc_missing_falls_back(tmp_path, caplog):
    """When working_copy_path is set but file missing, warn and fall back to source."""
    import logging
    from image_loader import get_canonical_image_path

    photo = {"working_copy_path": "working/99.jpg", "folder_id": 1, "filename": "src.jpg"}
    folders = {1: "/some/folder"}

    with caplog.at_level(logging.WARNING):
        result = get_canonical_image_path(photo, str(tmp_path), folders)

    assert result == "/some/folder/src.jpg"
    assert any("working copy missing" in r.message.lower() for r in caplog.records)
```

**Step 2: Run tests to verify they fail**

```bash
python -m pytest vireo/tests/test_image_loader.py -v -k canonical
```

Expected: all FAIL (function not defined).

**Step 3: Implement**

Add to `vireo/image_loader.py`:

```python
def get_canonical_image_path(photo, vireo_dir, folders):
    """Return the canonical image path for a photo — the root of the pyramid.

    Preference order:
      1. working copy JPEG (if photo.working_copy_path is set and file exists)
      2. source file (folder.path + '/' + photo.filename)

    If working_copy_path is set but the file is missing, logs a warning and
    falls back to source. Callers should still handle missing source files.

    Args:
        photo: dict with working_copy_path, folder_id, filename
        vireo_dir: path to ~/.vireo/
        folders: {folder_id: folder_path} mapping

    Returns:
        str path (may or may not exist — caller checks)
    """
    wc_rel = photo.get("working_copy_path")
    if wc_rel:
        wc_abs = os.path.join(vireo_dir, wc_rel)
        if os.path.exists(wc_abs):
            return wc_abs
        log.warning(
            "Canonical path: working copy missing for photo %s at %s; "
            "falling back to source", photo.get("id"), wc_abs,
        )
    folder_path = folders.get(photo["folder_id"], "")
    return os.path.join(folder_path, photo["filename"])
```

**Step 4: Run tests to verify they pass**

```bash
python -m pytest vireo/tests/test_image_loader.py -v -k canonical
```

Expected: all PASS.

**Step 5: Commit**

```bash
git add vireo/image_loader.py vireo/tests/test_image_loader.py
git commit -m "feat: add get_canonical_image_path helper"
```

---

## Task 5: Extract working copy for large JPEGs

**Files:**
- Modify: `vireo/scanner.py` (extend the working-copy extraction pass — search for `WHERE p.extension IN` near line 270)
- Test: `vireo/tests/test_scanner.py` (may need to create this; check first)

**Background:** Today, `scanner.py` extracts working copies only for photos whose extension is in `RAW_EXTENSIONS`. We extend the query to also select JPEGs where `max(width, height) > working_copy_max_size`. These photos get a working copy downsampled to the cap. The existing `extract_working_copy` in `image_loader.py` already handles JPEG sources generically (via `load_image`), so no change is needed there.

**Step 1: Find the exact lines**

Open `vireo/scanner.py` and find the block that begins with:

```python
rows = db.conn.execute(
    "SELECT p.id, p.filename, p.companion_path, p.working_copy_path, "
    ...
    "WHERE p.extension IN ({}) AND p.working_copy_path IS NULL".format(
        ",".join("?" for _ in RAW_EXTENSIONS)
    ),
    list(RAW_EXTENSIONS),
).fetchall()
```

This is the location to modify.

**Step 2: Write the failing test**

Create or extend a file at `vireo/tests/test_scanner_working_copy.py`:

```python
"""Working copy extraction for large JPEGs."""
import os
import tempfile
from PIL import Image


def _make_jpeg(path, width, height):
    img = Image.new("RGB", (width, height), (128, 128, 128))
    img.save(path, "JPEG", quality=85)


def test_extract_working_copy_for_large_jpeg(tmp_path):
    """A JPEG larger than working_copy_max_size gets a working copy created."""
    from db import Database
    from scanner import extract_missing_working_copies

    # Set up a fake folder and DB with a large JPEG photo row
    folder = tmp_path / "photos"
    folder.mkdir()
    src = folder / "big.jpg"
    _make_jpeg(str(src), 6000, 4000)  # larger than default 4096 cap

    vireo_dir = tmp_path / "vireo"
    vireo_dir.mkdir()
    db = Database(str(vireo_dir / "test.db"))
    folder_id = db.add_folder(str(folder))
    photo_id = db.add_photo(folder_id, "big.jpg", "jpg",
                             width=6000, height=4000)

    extract_missing_working_copies(db, str(vireo_dir),
                                    wc_max_size=4096, wc_quality=92)

    # Working copy created
    wc_path = vireo_dir / "working" / f"{photo_id}.jpg"
    assert wc_path.exists()
    with Image.open(wc_path) as img:
        assert max(img.size) == 4096

    # DB row updated
    row = db.conn.execute(
        "SELECT working_copy_path FROM photos WHERE id=?", (photo_id,)
    ).fetchone()
    assert row["working_copy_path"] == f"working/{photo_id}.jpg"


def test_no_working_copy_for_small_jpeg(tmp_path):
    """A JPEG within the cap does NOT get a working copy."""
    from db import Database
    from scanner import extract_missing_working_copies

    folder = tmp_path / "photos"
    folder.mkdir()
    src = folder / "small.jpg"
    _make_jpeg(str(src), 2000, 1500)  # below 4096

    vireo_dir = tmp_path / "vireo"
    vireo_dir.mkdir()
    db = Database(str(vireo_dir / "test.db"))
    folder_id = db.add_folder(str(folder))
    photo_id = db.add_photo(folder_id, "small.jpg", "jpg",
                             width=2000, height=1500)

    extract_missing_working_copies(db, str(vireo_dir),
                                    wc_max_size=4096, wc_quality=92)

    # No working copy
    wc_path = vireo_dir / "working" / f"{photo_id}.jpg"
    assert not wc_path.exists()
    row = db.conn.execute(
        "SELECT working_copy_path FROM photos WHERE id=?", (photo_id,)
    ).fetchone()
    assert row["working_copy_path"] is None
```

Note: the name `extract_missing_working_copies` is the existing function that currently only handles RAW. If the function name differs, grep `def ` in `scanner.py` to find it; the plan assumes this name — adjust if needed.

**Step 3: Run tests to verify they fail**

```bash
python -m pytest vireo/tests/test_scanner_working_copy.py -v
```

Expected: FAIL (large-JPEG test fails because no working copy is created).

**Step 4: Implement**

In `vireo/scanner.py`, find the `WHERE p.extension IN ({}) AND p.working_copy_path IS NULL` query and replace it with a two-part selector:

```python
# Select photos that need a working copy:
# - All RAW files without one
# - Large JPEGs (width or height exceeds working_copy_max_size) without one
placeholders = ",".join("?" for _ in RAW_EXTENSIONS)
rows = db.conn.execute(
    f"""
    SELECT p.id, p.filename, p.companion_path, p.working_copy_path,
           p.extension, p.width, p.height, f.path AS folder_path
      FROM photos p
      JOIN folders f ON p.folder_id = f.id
     WHERE p.working_copy_path IS NULL
       AND (
           p.extension IN ({placeholders})
        OR (LOWER(p.extension) IN ('jpg', 'jpeg')
             AND (p.width > ? OR p.height > ?))
       )
    """,
    list(RAW_EXTENSIONS) + [wc_max_size, wc_max_size],
).fetchall()
```

The existing loop body below (`for _i, row in enumerate(rows): ...`) doesn't need changes — `extract_working_copy` in `image_loader.py` already handles JPEG sources generically via `load_image`.

Also: confirm the surrounding function signature accepts `wc_max_size` and `wc_quality`. If the call site in `scan()` currently passes these, nothing more to do. If not, thread them through.

**Step 5: Run tests to verify they pass**

```bash
python -m pytest vireo/tests/test_scanner_working_copy.py -v
```

Expected: PASS both.

**Step 6: Commit**

```bash
git add vireo/scanner.py vireo/tests/test_scanner_working_copy.py
git commit -m "feat: extract working copy for JPEGs larger than the cap"
```

---

## Task 6: Refactor thumbnail generation to use canonical helper

**Files:**
- Modify: `vireo/thumbnails.py` (`generate_all` loop around lines 93-103)
- Test: extend `vireo/tests/test_thumbnails.py` if it exists, else add a small one

**Background:** `thumbnails.generate_all` already has ad-hoc "prefer working copy" logic at lines 94-102. Replace that block with a call to `get_canonical_image_path` so the rule is in one place and the codebase has a single canonical-resolution path.

**Step 1: Write the failing test**

Add to `vireo/tests/test_thumbnails.py` (create if absent):

```python
def test_generate_all_uses_canonical_path(tmp_path, monkeypatch):
    """generate_all reads through get_canonical_image_path."""
    import thumbnails
    calls = []
    real = thumbnails.generate_thumbnail

    def spy(photo_id, source_path, cache_dir, **kw):
        calls.append(source_path)
        return real(photo_id, source_path, cache_dir, **kw)

    monkeypatch.setattr(thumbnails, "generate_thumbnail", spy)

    # ... set up a Database with one photo that has a working_copy_path
    # ... call thumbnails.generate_all(db, cache_dir, vireo_dir=vireo_dir)
    # Assert the source_path passed in == the working copy path

    # (Fill in fixture setup in the style of test_scanner_working_copy.py)
```

Fill in fixture details to match your existing test style; the key behavior is: given a photo with `working_copy_path` set to an existing file, the first (only) call to `generate_thumbnail` receives that path as `source_path`.

**Step 2: Run test to verify it fails**

Run your new test. Expected: may PASS already (existing code does prefer working copy), but FAIL once you assert the spy calls are routed through `get_canonical_image_path` specifically. To force a FAIL, additionally mock `get_canonical_image_path` and assert it's called:

```python
from unittest.mock import MagicMock
mock_helper = MagicMock(return_value="/tmp/fake-canonical.jpg")
monkeypatch.setattr("thumbnails.get_canonical_image_path", mock_helper)
thumbnails.generate_all(...)
assert mock_helper.called
```

Expected: FAIL (thumbnails.py doesn't import or use `get_canonical_image_path`).

**Step 3: Implement**

In `vireo/thumbnails.py`, at the top:

```python
from image_loader import get_canonical_image_path, load_image
```

Replace the block at lines 94-103 (the ad-hoc "prefer working copy" resolution) with:

```python
for i, photo in enumerate(needed):
    source_path = get_canonical_image_path(photo, vireo_dir, folders) \
        if vireo_dir else os.path.join(folders.get(photo["folder_id"], ""),
                                       photo["filename"])
    if generate_thumbnail(photo["id"], source_path, cache_dir,
                          size=thumb_size, quality=thumb_quality) is not None:
        generated += 1
    else:
        failed += 1
    if progress_callback:
        progress_callback(i + 1, total)
```

The `if vireo_dir else ...` branch preserves behavior when a caller doesn't pass `vireo_dir` (backward compat; the helper requires it). Grep for `generate_all(` callers to confirm they all pass `vireo_dir` — if so, drop the fallback.

**Step 4: Run test to verify it passes**

```bash
python -m pytest vireo/tests/test_thumbnails.py -v
```

Expected: PASS.

**Step 5: Commit**

```bash
git add vireo/thumbnails.py vireo/tests/test_thumbnails.py
git commit -m "refactor: thumbnail generation uses canonical path helper"
```

---

## Task 7: Unified preview handler

**Files:**
- Modify: `vireo/app.py` (`serve_photo_preview` at line 7085)
- Test: `vireo/tests/test_photos_api.py`

**Background:** `serve_photo_preview` currently does its own working-copy resolution, its own cache check via `os.path.exists`, and writes the file unconditionally. Refactor it to:

1. Resolve canonical path via `get_canonical_image_path`.
2. Check `preview_cache` table for a tracked row; if found, `touch` and serve.
3. If file exists on disk but no row: lazily adopt (insert row with `bytes=st_size`, `last_access_at=st_mtime`), touch, serve.
4. If neither: load canonical, resize, save, insert row, evict-if-over-quota, serve.

**Step 1: Write the failing tests**

Add to `vireo/tests/test_photos_api.py`:

```python
def test_preview_cache_miss_creates_row(client_with_photo):
    """First request to a size inserts a preview_cache row."""
    app, db, photo_id = client_with_photo
    client = app.test_client()
    resp = client.get(f"/photos/{photo_id}/preview?size=1920")
    assert resp.status_code == 200
    row = db.preview_cache_get(photo_id, 1920)
    assert row is not None
    assert row["bytes"] > 0


def test_preview_cache_hit_updates_last_access(client_with_photo):
    """Second request touches last_access_at."""
    import time
    app, db, photo_id = client_with_photo
    client = app.test_client()
    client.get(f"/photos/{photo_id}/preview?size=1920")
    row1 = db.preview_cache_get(photo_id, 1920)
    time.sleep(0.01)
    client.get(f"/photos/{photo_id}/preview?size=1920")
    row2 = db.preview_cache_get(photo_id, 1920)
    assert row2["last_access_at"] > row1["last_access_at"]


def test_preview_adopts_existing_file_on_first_access(client_with_photo, tmp_path):
    """A cached file left over from the old scheme is adopted into the LRU."""
    import os, time
    app, db, photo_id = client_with_photo
    # Create a cache file manually without a DB row
    preview_dir = os.path.join(
        os.path.dirname(app.config["THUMB_CACHE_DIR"]), "previews"
    )
    os.makedirs(preview_dir, exist_ok=True)
    cache_path = os.path.join(preview_dir, f"{photo_id}_1920.jpg")
    with open(cache_path, "wb") as f:
        f.write(b"x" * 12345)
    # Backdate mtime
    past = time.time() - 3600
    os.utime(cache_path, (past, past))
    assert db.preview_cache_get(photo_id, 1920) is None

    client = app.test_client()
    resp = client.get(f"/photos/{photo_id}/preview?size=1920")
    assert resp.status_code == 200
    row = db.preview_cache_get(photo_id, 1920)
    assert row is not None
    assert row["bytes"] == 12345
```

The `client_with_photo` fixture should set up a Flask test client with a temp DB and one real photo row (ideally a small fixture JPEG). Check existing tests in `test_photos_api.py` for the pattern.

**Step 2: Run tests to verify they fail**

```bash
python -m pytest vireo/tests/test_photos_api.py -v -k "preview_cache_ or adopts"
```

Expected: FAIL (no LRU tracking yet).

**Step 3: Refactor the handler**

Replace the body of `serve_photo_preview` in `vireo/app.py` (approx. lines 7086-7143) with:

```python
@app.route("/photos/<int:photo_id>/preview")
def serve_photo_preview(photo_id):
    """Serve a JPEG preview at a chosen max-size.

    Cache is LRU-tracked in the preview_cache table; on-disk files that
    predate this scheme are adopted lazily on first access.
    """
    import os
    import config as cfg
    from flask import request, send_file

    try:
        size = int(request.args.get("size", "1920"))
    except ValueError:
        return "Invalid size", 400
    if size not in allowed_preview_sizes():
        return "Unsupported size", 400

    db = _get_db()
    photo = db.get_photo(photo_id)
    if not photo:
        return "Not found", 404

    vireo_dir = os.path.dirname(app.config["THUMB_CACHE_DIR"])
    preview_dir = os.path.join(vireo_dir, "previews")
    cache_path = os.path.join(preview_dir, f"{photo_id}_{size}.jpg")

    # Cache hit (tracked): touch and serve
    if db.preview_cache_get(photo_id, size) and os.path.exists(cache_path):
        db.preview_cache_touch(photo_id, size)
        return send_file(cache_path, mimetype="image/jpeg")

    # Cache hit (on-disk but untracked): lazy adoption
    if os.path.exists(cache_path):
        import time
        st = os.stat(cache_path)
        db.conn.execute(
            "INSERT OR REPLACE INTO preview_cache "
            "(photo_id, size, bytes, last_access_at) VALUES (?, ?, ?, ?)",
            (photo_id, size, st.st_size, st.st_mtime),
        )
        db.conn.commit()
        db.preview_cache_touch(photo_id, size)
        return send_file(cache_path, mimetype="image/jpeg")

    # Cache miss: generate, insert, evict
    from image_loader import _load_standard, get_canonical_image_path, load_image

    folders = {f["id"]: f["path"] for f in db.get_folder_tree()}
    canonical = get_canonical_image_path(photo, vireo_dir, folders)

    # Canonical is always JPEG-friendly when it's a working copy;
    # fall back to load_image (which handles RAW) if it isn't.
    img = _load_standard(canonical, size) if canonical.lower().endswith(
        (".jpg", ".jpeg")
    ) else load_image(canonical, max_size=size)
    if img is None:
        return "Could not load image", 500

    os.makedirs(preview_dir, exist_ok=True)
    preview_quality = cfg.load().get("preview_quality", 90)
    img.save(cache_path, format="JPEG", quality=preview_quality)

    bytes_ = os.path.getsize(cache_path)
    db.preview_cache_insert(photo_id, size, bytes_)
    evict_preview_cache_if_over_quota(db, vireo_dir)

    return send_file(cache_path, mimetype="image/jpeg")
```

Add at module level (inside `create_app`, near the `PREVIEW_SIZE_ALLOWLIST` constant):

```python
def allowed_preview_sizes():
    """Allowlist for /photos/<id>/preview?size=N.

    Includes the fixed tier plus the user-configured preview_max_size
    so /full can delegate here.
    """
    import config as cfg
    fixed = {1920, 2560, 3840}
    pm = cfg.get("preview_max_size") or 1920
    if pm == 0:
        return fixed  # 0 means "full" — /full handles it without this path
    return fixed | {int(pm)}
```

Leave `evict_preview_cache_if_over_quota` as a forward reference — it lands in Task 9. Add a stub for now:

```python
def evict_preview_cache_if_over_quota(db, vireo_dir):
    """Evict until under preview_cache_max_mb. Implemented in Task 9."""
    return
```

**Step 4: Run tests to verify they pass**

```bash
python -m pytest vireo/tests/test_photos_api.py -v -k "preview_cache_ or adopts"
```

Expected: PASS.

**Step 5: Commit**

```bash
git add vireo/app.py vireo/tests/test_photos_api.py
git commit -m "refactor: unified preview handler with preview_cache LRU tracking"
```

---

## Task 8: Make `/full` an alias for `/preview?size=<preview_max_size>`

**Files:**
- Modify: `vireo/app.py` (`serve_full_photo` at line 7032)
- Test: `vireo/tests/test_photos_api.py`

**Step 1: Write the failing test**

Add to `vireo/tests/test_photos_api.py`:

```python
def test_full_is_alias_for_preview_at_configured_size(client_with_photo, monkeypatch):
    """/full returns the same bytes as /preview?size=<preview_max_size>."""
    import config as cfg
    # Pin preview_max_size to 1920 for determinism
    monkeypatch.setattr(cfg, "get",
                         lambda k: 1920 if k == "preview_max_size" else cfg.DEFAULTS.get(k))
    app, db, photo_id = client_with_photo
    client = app.test_client()
    full = client.get(f"/photos/{photo_id}/full").data
    preview = client.get(f"/photos/{photo_id}/preview?size=1920").data
    assert full == preview
```

**Step 2: Run test to verify it fails**

```bash
python -m pytest vireo/tests/test_photos_api.py::test_full_is_alias_for_preview_at_configured_size -v
```

Expected: FAIL (`/full` uses a separate code path and separate cache file `{id}.jpg`).

**Step 3: Implement**

Replace the body of `serve_full_photo` in `vireo/app.py`:

```python
@app.route("/photos/<int:photo_id>/full")
def serve_full_photo(photo_id):
    """Serve a display-sized preview (alias for /preview at preview_max_size)."""
    import config as cfg
    from flask import request

    size = cfg.get("preview_max_size") or 1920
    if size == 0:
        # preview_max_size = 0 historically meant "full" — route to /original
        from flask import redirect
        return redirect(f"/photos/{photo_id}/original")

    # Delegate internally to serve_photo_preview without an HTTP redirect
    # (cheaper and keeps cookies/headers intact).
    request.args = request.args.copy()  # ensure mutable
    request.args = {**request.args, "size": str(size)}
    return serve_photo_preview(photo_id)
```

Note: Flask's `request.args` is immutable by default. A cleaner approach is to extract the preview-serving logic into an internal helper `_serve_preview(photo_id, size)` and call that from both routes. Refactor if the in-place `request.args` manipulation feels hacky:

```python
def _serve_preview(photo_id, size):
    # Body of the Task 7 refactor goes here.
    ...

@app.route("/photos/<int:photo_id>/preview")
def serve_photo_preview(photo_id):
    try:
        size = int(request.args.get("size", "1920"))
    except ValueError:
        return "Invalid size", 400
    if size not in allowed_preview_sizes():
        return "Unsupported size", 400
    return _serve_preview(photo_id, size)

@app.route("/photos/<int:photo_id>/full")
def serve_full_photo(photo_id):
    size = cfg.get("preview_max_size") or 1920
    if size == 0:
        return redirect(f"/photos/{photo_id}/original")
    return _serve_preview(photo_id, size)
```

Prefer the helper approach.

**Step 4: Run test to verify it passes**

```bash
python -m pytest vireo/tests/test_photos_api.py::test_full_is_alias_for_preview_at_configured_size -v
```

Expected: PASS.

**Step 5: Also delete the now-redundant `{id}.jpg` cache path convention**

No code deletes existing `{id}.jpg` files; they'll sit in `~/.vireo/previews/` harmlessly until the user clears the cache (Task 11). Document in the PR description that users may want to run "Clear cache" after upgrading to reclaim disk.

**Step 6: Commit**

```bash
git add vireo/app.py vireo/tests/test_photos_api.py
git commit -m "refactor: /full is now an alias for /preview at configured size"
```

---

## Task 9: LRU eviction

**Files:**
- Modify: `vireo/app.py` (implement `evict_preview_cache_if_over_quota` — replace stub from Task 7)
- Test: `vireo/tests/test_photos_api.py`

**Step 1: Write the failing test**

Add to `vireo/tests/test_photos_api.py`:

```python
def test_eviction_removes_oldest_files_when_over_quota(client_with_photos, monkeypatch):
    """When writes push cache over quota, oldest-accessed entries are evicted."""
    import os, time
    import config as cfg
    # Tiny quota so two small previews push us over
    monkeypatch.setitem(cfg.DEFAULTS, "preview_cache_max_mb", 0)  # 0 MB → 0 bytes
    # Actually set a very small positive value via a custom loader
    orig_load = cfg.load
    monkeypatch.setattr(cfg, "load",
                         lambda: {**orig_load(), "preview_cache_max_mb": 0})

    app, db, photo_ids = client_with_photos  # fixture returns >= 2 photos
    client = app.test_client()

    # Generate previews for photo_ids[0] and photo_ids[1]
    client.get(f"/photos/{photo_ids[0]}/preview?size=1920")
    time.sleep(0.01)
    client.get(f"/photos/{photo_ids[1]}/preview?size=1920")

    # Either (a) both were evicted because quota was 0, OR
    # (b) only the oldest (photo_ids[0]) was evicted.
    # Since quota=0 the correct behavior is everything evicted.
    assert db.preview_cache_total_bytes() == 0
    vireo_dir = os.path.dirname(app.config["THUMB_CACHE_DIR"])
    for pid in photo_ids[:2]:
        assert not os.path.exists(os.path.join(vireo_dir, "previews", f"{pid}_1920.jpg"))
```

**Step 2: Run test to verify it fails**

```bash
python -m pytest vireo/tests/test_photos_api.py::test_eviction_removes_oldest_files_when_over_quota -v
```

Expected: FAIL (stub doesn't evict).

**Step 3: Implement eviction**

Replace the stub in `vireo/app.py` from Task 7 with:

```python
def evict_preview_cache_if_over_quota(db, vireo_dir):
    """Evict oldest preview_cache entries until under preview_cache_max_mb.

    Walks rows in ascending last_access_at order, removes file + row,
    stops as soon as total_bytes <= quota. Self-healing: if the file is
    already missing, we still delete the row.
    """
    import os
    import config as cfg
    quota_mb = cfg.load().get("preview_cache_max_mb", 2048)
    max_bytes = int(quota_mb) * 1024 * 1024
    total = db.preview_cache_total_bytes()
    if total <= max_bytes:
        return

    preview_dir = os.path.join(vireo_dir, "previews")
    for row in db.preview_cache_oldest_first():
        if total <= max_bytes:
            break
        path = os.path.join(preview_dir, f"{row['photo_id']}_{row['size']}.jpg")
        try:
            os.remove(path)
        except FileNotFoundError:
            pass
        db.preview_cache_delete(row["photo_id"], row["size"])
        total -= row["bytes"]
```

**Step 4: Run test to verify it passes**

```bash
python -m pytest vireo/tests/test_photos_api.py::test_eviction_removes_oldest_files_when_over_quota -v
```

Expected: PASS.

**Step 5: Commit**

```bash
git add vireo/app.py vireo/tests/test_photos_api.py
git commit -m "feat: evict preview_cache entries when over configured quota"
```

---

## Task 10: `/api/preview-cache` uses DB totals + clear endpoint

**Files:**
- Modify: `vireo/app.py:2525-2539` (rewrite `api_preview_cache`)
- Add: new route `@app.route("/api/preview-cache/clear", methods=["POST"])`
- Test: `vireo/tests/test_photos_api.py`

**Step 1: Write failing tests**

```python
def test_preview_cache_endpoint_uses_db(client_with_photo):
    """/api/preview-cache returns totals from preview_cache table, not filesystem."""
    app, db, photo_id = client_with_photo
    client = app.test_client()
    # Generate a preview to populate the table
    client.get(f"/photos/{photo_id}/preview?size=1920")

    resp = client.get("/api/preview-cache")
    assert resp.status_code == 200
    data = resp.get_json()
    assert data["count"] == 1
    assert data["total_size"] > 0
    assert "quota_bytes" in data


def test_preview_cache_clear_removes_all(client_with_photo):
    """POST /api/preview-cache/clear empties the table and files."""
    import os
    app, db, photo_id = client_with_photo
    client = app.test_client()
    client.get(f"/photos/{photo_id}/preview?size=1920")
    assert db.preview_cache_total_bytes() > 0

    resp = client.post("/api/preview-cache/clear")
    assert resp.status_code == 200

    assert db.preview_cache_total_bytes() == 0
    vireo_dir = os.path.dirname(app.config["THUMB_CACHE_DIR"])
    assert not os.path.exists(
        os.path.join(vireo_dir, "previews", f"{photo_id}_1920.jpg")
    )
```

**Step 2: Run tests to verify they fail**

```bash
python -m pytest vireo/tests/test_photos_api.py -v -k "preview_cache_endpoint or preview_cache_clear"
```

Expected: both FAIL.

**Step 3: Implement**

Replace `api_preview_cache` in `vireo/app.py`:

```python
@app.route("/api/preview-cache")
def api_preview_cache():
    """Return counts and totals from the preview_cache table, plus quota."""
    import config as cfg
    db = _get_db()
    count_row = db.conn.execute(
        "SELECT COUNT(*) AS c FROM preview_cache"
    ).fetchone()
    total = db.preview_cache_total_bytes()
    quota_mb = cfg.load().get("preview_cache_max_mb", 2048)
    return jsonify({
        "count": count_row["c"],
        "total_size": total,
        "quota_bytes": int(quota_mb) * 1024 * 1024,
    })


@app.route("/api/preview-cache/clear", methods=["POST"])
def api_preview_cache_clear():
    """Delete every preview_cache file and row."""
    import os
    db = _get_db()
    vireo_dir = os.path.dirname(app.config["THUMB_CACHE_DIR"])
    preview_dir = os.path.join(vireo_dir, "previews")
    rows = db.conn.execute(
        "SELECT photo_id, size FROM preview_cache"
    ).fetchall()
    for r in rows:
        path = os.path.join(preview_dir, f"{r['photo_id']}_{r['size']}.jpg")
        try:
            os.remove(path)
        except FileNotFoundError:
            pass
    db.conn.execute("DELETE FROM preview_cache")
    db.conn.commit()
    return jsonify({"cleared": len(rows)})
```

**Step 4: Run tests to verify they pass**

```bash
python -m pytest vireo/tests/test_photos_api.py -v -k "preview_cache_endpoint or preview_cache_clear"
```

Expected: PASS.

**Step 5: Commit**

```bash
git add vireo/app.py vireo/tests/test_photos_api.py
git commit -m "feat: /api/preview-cache reads from DB; add /clear endpoint"
```

---

## Task 11: Settings UI for cache quota

**Files:**
- Modify: `vireo/templates/settings.html` (find the "Thumbnails & Previews" section)
- Verify: settings form already posts `preview_cache_max_mb` to `/api/config` (existing handler should pick it up automatically since it's now in `DEFAULTS`)

**Step 1: Find the section**

Open `vireo/templates/settings.html` and locate the block that renders `preview_max_size` or `preview_quality`. The new field goes adjacent.

**Step 2: Add the form field + usage indicator**

Insert a new row modeled on the existing preview_max_size / preview_quality inputs:

```html
<tr>
  <td><label for="preview_cache_max_mb">Preview cache size (MB)</label></td>
  <td>
    <input type="number" id="preview_cache_max_mb"
           name="preview_cache_max_mb" min="0" step="128"
           value="{{ config.preview_cache_max_mb }}">
    <span class="cache-usage" id="previewCacheUsage">—</span>
    <button type="button" id="clearPreviewCacheBtn">Clear cache</button>
  </td>
</tr>
```

Add JS at the bottom of the settings template (or in the existing inline `<script>` block) to populate the usage span and wire the clear button:

```html
<script>
  (async () => {
    const span = document.getElementById('previewCacheUsage');
    const btn = document.getElementById('clearPreviewCacheBtn');
    async function refresh() {
      const r = await fetch('/api/preview-cache');
      const d = await r.json();
      const usedMb = (d.total_size / 1024 / 1024).toFixed(1);
      const quotaMb = (d.quota_bytes / 1024 / 1024).toFixed(0);
      span.textContent = `Current: ${usedMb} / ${quotaMb} MB (${d.count} files)`;
    }
    btn.addEventListener('click', async () => {
      if (!confirm('Delete all cached previews?')) return;
      await fetch('/api/preview-cache/clear', {method: 'POST'});
      refresh();
    });
    refresh();
  })();
</script>
```

**Step 3: Verify save handler round-trips**

Grep for where settings.html submits — the existing settings POST handler in `app.py` should merge the form fields into config. If `preview_cache_max_mb` is in `DEFAULTS`, the handler accepts it without additional code. Verify by starting the app manually, changing the value, and confirming `~/.vireo/config.json` updates.

**Step 4: Smoke test**

Start the app and exercise the UI:

```bash
python vireo/app.py --db /tmp/vireo-test.db --port 8080
```

Visit `http://localhost:8080/settings`, confirm:
- Field renders with current value.
- "Current: X.X / Y MB (N files)" displays after page load.
- "Clear cache" deletes all files, then the span shows 0.
- Changing the value and saving updates the config file.

**Step 5: Commit**

```bash
git add vireo/templates/settings.html
git commit -m "feat: expose preview_cache_max_mb in settings UI"
```

---

## Task 12: Trigger eviction when quota shrinks via settings save

**Files:**
- Modify: `vireo/app.py` (the settings POST handler — grep for `/api/config` or `update_config`)
- Test: `vireo/tests/test_app.py` or `vireo/tests/test_photos_api.py`

**Background:** When a user reduces `preview_cache_max_mb` via the settings page, run eviction immediately so the cache shrinks to the new quota without waiting for the next cache write.

**Step 1: Write failing test**

```python
def test_settings_save_triggers_eviction_when_quota_shrinks(client_with_photo):
    """POSTing a smaller preview_cache_max_mb evicts down to the new quota."""
    import config as cfg
    app, db, photo_id = client_with_photo
    client = app.test_client()

    # Populate cache
    client.get(f"/photos/{photo_id}/preview?size=1920")
    assert db.preview_cache_total_bytes() > 0

    # Shrink quota to 0
    resp = client.post("/api/config", json={"preview_cache_max_mb": 0})
    assert resp.status_code == 200

    assert db.preview_cache_total_bytes() == 0
```

**Step 2: Run test to verify it fails**

Expected: FAIL.

**Step 3: Implement**

Find the settings POST handler. After `cfg.save(...)`, if the request changed `preview_cache_max_mb`, invoke eviction:

```python
# Inside the settings-save handler:
if "preview_cache_max_mb" in changed_keys:
    import os
    vireo_dir = os.path.dirname(app.config["THUMB_CACHE_DIR"])
    evict_preview_cache_if_over_quota(_get_db(), vireo_dir)
```

If the handler doesn't track `changed_keys`, a simpler approach: always run eviction after every settings save (cheap — no-op when under quota).

**Step 4: Run test to verify it passes**

Expected: PASS.

**Step 5: Commit**

```bash
git add vireo/app.py vireo/tests/test_app.py
git commit -m "feat: evict preview cache when quota is reduced via settings"
```

---

## Task 13: Integration test — full pyramid cycle

**Files:**
- Add: `vireo/tests/test_preview_pyramid_integration.py`

**Step 1: Write the integration test**

```python
"""End-to-end: scan → working copy → thumbnail → preview → eviction."""
import os
from PIL import Image


def _make_jpeg(path, w, h, color=(200, 100, 50)):
    Image.new("RGB", (w, h), color).save(path, "JPEG", quality=85)


def test_full_pyramid_cycle(tmp_path, monkeypatch):
    import config as cfg
    from db import Database
    from app import create_app
    from scanner import extract_missing_working_copies
    from thumbnails import generate_all as gen_thumbs

    monkeypatch.setattr(cfg, "CONFIG_PATH", str(tmp_path / "config.json"))
    vireo_dir = tmp_path / "vireo"
    vireo_dir.mkdir()
    folder = tmp_path / "photos"
    folder.mkdir()
    big = folder / "big.jpg"
    small = folder / "small.jpg"
    _make_jpeg(str(big), 6000, 4000)
    _make_jpeg(str(small), 1500, 1000)

    db = Database(str(vireo_dir / "vireo.db"))
    fid = db.add_folder(str(folder))
    pid_big = db.add_photo(fid, "big.jpg", "jpg", width=6000, height=4000)
    pid_small = db.add_photo(fid, "small.jpg", "jpg", width=1500, height=1000)

    # 1. Scanner creates a working copy for the big JPEG only
    extract_missing_working_copies(db, str(vireo_dir),
                                    wc_max_size=4096, wc_quality=92)
    assert (vireo_dir / "working" / f"{pid_big}.jpg").exists()
    assert not (vireo_dir / "working" / f"{pid_small}.jpg").exists()

    # 2. Thumbnails read through canonical
    thumb_dir = vireo_dir / "thumbnails"
    gen_thumbs(db, str(thumb_dir), vireo_dir=str(vireo_dir))
    assert (thumb_dir / f"{pid_big}.jpg").exists()
    assert (thumb_dir / f"{pid_small}.jpg").exists()

    # 3. Preview endpoint generates + tracks
    app = create_app(str(vireo_dir / "vireo.db"), str(thumb_dir))
    client = app.test_client()
    resp = client.get(f"/photos/{pid_big}/preview?size=1920")
    assert resp.status_code == 200
    assert db.preview_cache_get(pid_big, 1920) is not None

    # 4. /full returns same bytes as preview?size=<configured>
    cfg.set("preview_max_size", 1920)
    full_bytes = client.get(f"/photos/{pid_big}/full").data
    preview_bytes = client.get(f"/photos/{pid_big}/preview?size=1920").data
    assert full_bytes == preview_bytes

    # 5. Eviction respects quota
    cfg.set("preview_cache_max_mb", 0)
    client.post("/api/config", json={"preview_cache_max_mb": 0})
    assert db.preview_cache_total_bytes() == 0
```

**Step 2: Run it**

```bash
python -m pytest vireo/tests/test_preview_pyramid_integration.py -v
```

Expected: PASS.

**Step 3: Commit**

```bash
git add vireo/tests/test_preview_pyramid_integration.py
git commit -m "test: end-to-end preview pyramid integration"
```

---

## Task 14: Full regression + PR

**Step 1: Run the full project test suite**

```bash
python -m pytest tests/test_workspaces.py vireo/tests/test_db.py \
    vireo/tests/test_app.py vireo/tests/test_photos_api.py \
    vireo/tests/test_edits_api.py vireo/tests/test_jobs_api.py \
    vireo/tests/test_darktable_api.py vireo/tests/test_config.py \
    vireo/tests/test_image_loader.py vireo/tests/test_thumbnails.py \
    vireo/tests/test_scanner_working_copy.py \
    vireo/tests/test_preview_pyramid_integration.py -v
```

Expected: all PASS.

**Step 2: Manual smoke in browser**

```bash
python vireo/app.py --db /tmp/vireo-preview-pyramid.db --port 8080
```

- Add a folder with some real RAWs + JPEGs.
- Run a scan.
- Verify thumbnails render (grid page).
- Open lightbox, toggle 1:1 zoom.
- Open settings, change `Preview cache size` to a very small value, save, confirm cache clears.

**Step 3: Push the branch**

```bash
git push -u origin claude/preview-pyramid
```

**Step 4: Open the PR**

```bash
gh pr create --title "feat: preview pyramid — derive all sizes from canonical working copy" \
  --body "$(cat <<'EOF'
## Summary

- **Working copy is now the canonical root for every cached derivative.** Thumbnails and all preview sizes read through a new `get_canonical_image_path` helper. For RAW files, one decode per photo, ever. For large JPEGs (>4096px), a capped working copy is now extracted at scan.
- **LRU-bounded disk cache for sized previews.** New `preview_cache` SQLite table tracks `(photo_id, size, bytes, last_access_at)`. Reads touch the timestamp; writes evict when over the configured quota.
- **Configurable quota.** New `preview_cache_max_mb` setting (default 2048 MB), exposed in the settings page with live usage indicator and "Clear cache" button. Shrinking the quota evicts immediately.
- **Lazy migration.** Existing `{id}_{size}.jpg` files are adopted into the LRU on first access (mtime seeds `last_access_at`). No forced upgrade.
- **`/full` is now an alias for `/preview?size=<preview_max_size>`.** One code path, one cache.

See `docs/plans/2026-04-16-preview-pyramid-design.md` for the full design.

## Test plan

- [x] All baseline tests pass.
- [x] New unit tests for `get_canonical_image_path`, `preview_cache` DB methods, JPEG working copy extraction, lazy adoption, eviction.
- [x] Integration test covering scan → thumbnails → previews → eviction cycle.
- [x] Manual smoke: grid render, lightbox zoom, settings save, cache clear.

🤖 Generated with [Claude Code](https://claude.com/claude-code)
EOF
)"
```

Expected: PR URL returned. Monitor CI + review feedback; push fixes to the same branch.

---

## Notes for executing engineer

- **Run tests after every task.** If a task's tests pass but unrelated tests now fail, investigate before committing the next task — tracing regressions across 14 commits is painful.
- **No `.amend` commits.** Each task is a distinct logical step; squash happens at merge time per the repo's workflow.
- **Use the worktree tree.** All file paths in this plan are relative to the worktree root at `.worktrees/preview-pyramid/`. Never edit files in the main checkout.
- **Skill references.** If you hit a bug during implementation, use `superpowers:systematic-debugging`. If a test race appears, use `superpowers:condition-based-waiting`.
- **Commit message style.** Match the repo's history — short imperative subjects, lowercase after the type prefix (`feat:`, `refactor:`, `test:`).
