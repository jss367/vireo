# Import Job Implementation Plan (import/process split PR 2)

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** A first-class import job that copies card → archive directly (no staging), hash-verifies every file, and catalogs photos at their final paths incrementally — so folders appear in the workspace *during* the import and a dead run leaves a valid partial catalog that a retry resumes.

**Architecture:** New `vireo/import_job.py` implementing a per-batch commit loop that reuses the existing primitives — `ingest.discover_source_files` / `build_destination_path`, `import_dedup.DuplicateChecker`, `scanner.scan(restrict_dirs=…, restrict_files=…)` — but **not** `ingest()`'s monolithic loop (that stays untouched for the legacy pipeline until PR 4 deletes it). The core invariant from the design doc: *a photo row is created only when its file verifiably exists at its final archive path.* Exposed as job type `"import"` via `POST /api/jobs/import-photos`; the existing `POST /api/jobs/import` Lightroom catalog route remains unchanged. API-only in this PR; the Import page and chaining come in PR 3.

**Tech Stack:** Python 3, Flask, SQLite (raw cursor), pytest.

**Design doc:** `docs/plans/2026-07-04-import-process-split-design.md` (merged #1101)
**Parent plan:** `docs/plans/2026-07-04-import-process-split-plan.md` (merged #1102, Phase 2 skeleton)

**Test command (run before the PR):**
```bash
python -m pytest tests/test_workspaces.py vireo/tests/test_db.py vireo/tests/test_app.py vireo/tests/test_photos_api.py vireo/tests/test_edits_api.py vireo/tests/test_jobs_api.py vireo/tests/test_darktable_api.py vireo/tests/test_config.py vireo/tests/test_process_strategies.py vireo/tests/test_pipeline_job.py vireo/tests/test_import_job.py vireo/tests/test_ingest.py vireo/tests/test_import_dedup.py vireo/tests/test_scanner.py -v
```
`vireo/tests/test_import_job.py` is created in Task 2.1; until then pytest reports it as a collection error, which disappears once the module lands.

**Key existing code (verified 2026-07-04):**
- `vireo/ingest.py:293` — `ingest()` copies with `shutil.copy2` (numeric-suffix collision handling), shares a `DuplicateChecker`, returns `{copied, skipped_duplicate, failed, total, copied_paths, duplicate_folders}`. **No post-copy verification.** `discover_source_files(source_dir, file_types, recursive)` at :218; `build_destination_path(exif_timestamp, template)` at :125; `_is_unsafe_path` guards templates.
- `vireo/import_dedup.py` — `DuplicateChecker.match(source_file)` returns `("hash", h)`, `("key", k)`, or `None`; `.record(source_file)` marks a copied file known; `.content_hash()` is a **cached SHA-256** (`compute_file_hash`). `CatalogIndex.from_db(db)` builds the oracle.
- `vireo/scanner.py:1084` — `scan(root, db, …, restrict_dirs=…, restrict_files=…, vireo_dir=…, thumb_cache_dir=…, photo_callback=…, cancel_check=…)`. Restricted scans mark the restricted dirs as workspace roots and keep the base linked `is_root=0` (scanner.py:1356 comment) — this is the designed copy-import behavior and what the new-images walk expects. `photo_callback(photo_id, path_str)` fires per committed photo.
- `vireo/scanner.py:792` — `_extract_working_copies(db, vireo_dir, scope=…)`: working copies land at `<vireo_dir>/working/<photo_id>.jpg`, prefer the companion JPEG, skip previously-failed rows. `image_loader.extract_working_copy(source_path, output_path, max_size, quality)` at image_loader.py:511 takes an arbitrary source path — the read-from-card hook.
- `vireo/db.py` photos columns: `file_hash`, `hash_checked_at`, `hash_status` already exist (plus `working_copy_path`).
- `vireo/jobs.py:362` — `JobRunner.start(job_type, …)` docstring already lists `'import'` as an expected type.
- `vireo/move.py:63` — `_copy_and_verify` is **size-only**; import verification must be hash-based (Task 2.1), not this.
- Remote plumbing: `build_remote_move_spec` / `rsync_dest_spec` / `_remote_verify_complete` in `move.py`; fake-rsync test harness in `vireo/tests/test_move_remote.py`.

---

### Task 2.0: Reconnaissance (no code changes)

Answer four questions and record them in `vireo/import_job.py`'s module docstring in Task 2.1:

1. **Does `scan()` populate `photos.file_hash`?** Grep `file_hash` in `scanner.py`. If scan leaves it NULL, the import job owns writing it (Task 2.2 already assumes so; adjust if scan writes its own).
2. **Can `CatalogIndex` surface the matched photo's archive path for a `("key", …)` match?** Task 2.3's second-pass verification needs the cataloged twin's path. Check what `CatalogIndex.from_db` retains; if it only keeps keys/hashes, extend it minimally (store path alongside) or resolve the twin with a direct `stored_metadata_key`-based DB query at verification time — prefer whichever touches less code.
3. **How does the scan job wire `cancel_check` / `runner.is_cancelled`?** (See `_build_scan_work` in app.py.) The import job mirrors that convention.
4. **Batch size**: confirm `scan(restrict_dirs=…)` cost is dominated by the restricted dirs, not the archive tree (it should be — discovery is restricted). Pick the batch unit: all files landing in the same destination (template) folder, processed in template order, capped at ~200 files per scan call. Record the choice.

### Task 2.1: `vireo/import_job.py` — hash-verified copy primitive

**Files:**
- Create: `vireo/import_job.py`
- Test: `vireo/tests/test_import_job.py` (new)

**Step 1: Failing tests**

```python
"""Import job: copy card -> archive with hash verification."""
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))


def test_copy_and_hash_verify_roundtrip(tmp_path):
    from import_dedup import compute_file_hash
    from import_job import copy_and_hash_verify

    src = tmp_path / "card" / "DSC_0001.jpg"
    src.parent.mkdir()
    src.write_bytes(b"pixels" * 1000)
    dst = tmp_path / "archive" / "2026" / "DSC_0001.jpg"

    ok, file_hash = copy_and_hash_verify(str(src), str(dst))
    assert ok is True
    assert file_hash == compute_file_hash(str(src))
    assert dst.read_bytes() == src.read_bytes()


def test_copy_and_hash_verify_detects_corruption(tmp_path, monkeypatch):
    """A copy whose destination bytes differ must fail without deleting any
    previously verified archive file at the destination path."""
    import shutil

    from import_job import copy_and_hash_verify

    src = tmp_path / "card" / "DSC_0002.jpg"
    src.parent.mkdir()
    src.write_bytes(b"good bytes")
    dst = tmp_path / "archive" / "DSC_0002.jpg"
    dst.parent.mkdir()
    dst.write_bytes(b"existing verified archive bytes")

    real_copy2 = shutil.copy2

    def corrupting_copy2(s, d):
        real_copy2(s, d)
        with open(d, "r+b") as f:
            f.write(b"BAD")

    monkeypatch.setattr("import_job.shutil.copy2", corrupting_copy2)
    ok, file_hash = copy_and_hash_verify(str(src), str(dst))
    assert ok is False
    assert dst.read_bytes() == b"existing verified archive bytes"
    assert not list(dst.parent.glob(".DSC_0002.jpg.*.tmp"))
```

**Step 2:** Run `python -m pytest vireo/tests/test_import_job.py -v` — expect `ModuleNotFoundError`.

**Step 3:** Implement `copy_and_hash_verify(src, dst, *, src_hash=None)`:
- `os.makedirs(dirname, exist_ok=True)`; copy to a sibling temp path such as `.DSC_0002.jpg.<uuid>.tmp` with `shutil.copy2`; hash that temp copy with `compute_file_hash`; compare against `src_hash` (caller may pass the DuplicateChecker's cached hash) or a fresh source hash.
- On mismatch: remove only the temp copy, leave any existing `dst` untouched, and return `(False, None)`.
- On match: replace/promote the temp copy into `dst` with `os.replace`, preserving the verified file's metadata from `shutil.copy2`, and return `(True, file_hash)`.
- fsync is not required (the hash re-read after copy is the integrity gate, and a torn temp file fails it before it can replace the archive path).

**Step 4:** Run — PASS. **Step 5: Commit** — `feat: hash-verified copy primitive for the import job`

### Task 2.2: `run_import_job` — per-batch commit loop

**Files:**
- Modify: `vireo/import_job.py`
- Test: `vireo/tests/test_import_job.py`

**Step 1: Failing test** — the core invariant, written against the public entry point:

```python
def _make_card(tmp_path, names):
    """A fake card with tiny JPEGs can use distinct mtimes for folder planning:
    ingest._source_file_timestamps falls back to file mtime when EXIF is absent
    before build_destination_path formats the destination folder."""
```

- Fixture: card with 4 JPEGs (2 destined for one template folder, 2 for another via distinct mtimes), archive destination inside tmp_path, `Database` with active workspace.
- Call `run_import_job(job, runner, db_path, ws_id, ImportParams(sources=[card], destination=str(archive), folder_template="%Y/%Y-%m-%d"))` (mirror `run_pipeline_job`'s calling convention and the FakeRunner from test_pipeline_job.py).
- Assert: every discovered file exists under the archive at its template path; a photo row exists **for each copied file at its final path** (join folders on path); each row has `file_hash` set, `hash_status = 'verified'`, `hash_checked_at` non-NULL; the result dict reports `{discovered, copied, verified, skipped_duplicate, failed}` consistently; folders are linked to the active workspace.
- Duplicate-only variant: pre-catalog files at the archive destination without linking their folders to the active workspace, import a card containing only those duplicates, and assert the matched destination folders are scanned and linked even though no fresh files were copied.
- **Invariant assertion:** no photo row exists whose file is missing on disk (catalog ⊆ verified files).

**Step 2:** Run — FAIL (no `run_import_job`).

**Step 3:** Implement:
- `ImportParams` dataclass: `sources`, `destination`, `folder_template="%Y/%Y-%m-%d"`, `file_types="both"`, `skip_duplicates=True`, `verify_by_hash=False`, `recursive=True`, `after_import=None` (stored, unused until PR 3).
- Discover all files across sources (`discover_source_files`), resolve folder-planning timestamps with `ingest._source_file_timestamps` (EXIF first, file mtime fallback; do **not** use `source_capture_timestamps` directly here because it intentionally returns `None` for no-EXIF files), group into batches by destination folder (Task 2.0's batch unit), template order.
- One shared `DuplicateChecker(CatalogIndex.from_db(db), verify_by_hash=params.verify_by_hash)` across the whole run.
- Per file in a batch: dedup `match()` → record skip with its token and the matched archive folder path (Task 2.3 consumes the token; this task consumes the folder for workspace linking); else `copy_and_hash_verify` (pass the checker's cached hash when it has one); collision handling mirrors `ingest()` (numeric suffix when same name + different content at destination; skip when the destination file is byte-identical — reuse `ingest()`'s existing check shape).
- Per batch, after all copies: `scan(root=destination, db, restrict_dirs=[batch dest dir plus any duplicate-match destination dirs], restrict_files=[verified dest paths], vireo_dir=…, thumb_cache_dir=…, cancel_check=…)`, collecting `(photo_id, path)` via `photo_callback`; then `UPDATE photos SET file_hash=?, hash_status='verified', hash_checked_at=? WHERE id=?` for each freshly verified copy. This keeps duplicate-only imports from reporting success while leaving an unlinked workspace empty.
- `checker.record()` after each verified copy so intra-run duplicates (two cards, same shot) skip correctly.
- Progress: `runner.update_step`/`push_event` per batch with `"<folder>: copied/verified counts"` (per-folder progress is the PR 3 UI's data source).

**Step 4:** Run — PASS. **Step 5: Commit** — `feat: run_import_job copies, verifies, and catalogs per batch`

### Task 2.3: Safe-to-format ledger

**Files:**
- Modify: `vireo/import_job.py`
- Test: `vireo/tests/test_import_job.py`

**Step 1: Failing tests** (this is the P1 the plan review flagged — metadata-only matches must not count as safe):

- Fresh-copy run → result `safe_to_format is True`, `unsafe_files == []`.
- A card file whose `match()` is `("hash", …)` (byte-identical twin already cataloged) → still safe.
- A card file that key-matches (same name, size, trusted capture time) a cataloged twin **whose bytes differ** → `safe_to_format is False` and the file is listed in `unsafe_files` with a reason. Build the fixture from `test_import_dedup.py`'s `_write_jpeg(..., exif_dt=...)` and `_seed_photo(...)` pattern: seed the catalog row with the same trustworthy `photos.timestamp` (for example `2026-05-01T10:15:30`) and create the card file with matching `DateTimeOriginal`, name, and size but different bytes. Do not rely on mtime alone; `DuplicateChecker` key matches use EXIF/catalog capture timestamps.
- A failed copy → unsafe.

**Step 2:** Run — FAIL.

**Step 3:** Implement the per-file ledger: every discovered file ends in exactly one bucket — `verified_fresh`, `verified_duplicate` (hash-backed), `needs_hash_check` (key-only match), `failed`. After the copy loop, second-pass the `needs_hash_check` bucket: hash the card file, hash the cataloged twin at its archive path (Task 2.0 answered how to resolve it), equal → `verified_duplicate`, else unsafe. `safe_to_format = not unsafe and no failures and every file bucketed`. Result carries counts + `unsafe_files` (path + reason) — a pill that means exactly what it says.

**Step 4:** Run — PASS. **Step 5: Commit** — `feat: hash-backed safe-to-format ledger`

### Task 2.4: Job type + `POST /api/jobs/import-photos`

**Files:**
- Modify: `vireo/app.py`
- Test: `vireo/tests/test_jobs_api.py`

**Step 1: Failing tests** (mirror the pipeline route's validation tests):
- Existing Lightroom route remains intact: `POST /api/jobs/import` with `{catalogs, strategy, write_xmp}` still starts the Lightroom catalog import job and is not shadowed by the new photo import route.
- Happy path: POST `/api/jobs/import-photos` with `{sources: [card], destination, after_import: "cull_ready"}` → 200, `job_id` starts with `import-`; job config records sources/destination/template/`after_import`; after completion (use `wait_for_job_via_client`) the result carries the Task 2.2/2.3 counts.
- `after_import: null` → 200 (import-only; PR 3's hook short-circuits — same nullable vocabulary as `pipeline.default_strategy`).
- `after_import: "yolo"` and `"none"` → 400 via `resolve_strategy` (validate at enqueue, not at completion — failing the chain hours later is the old pipeline's mistake).
- Missing sources / missing destination / relative destination / `.photoslibrary` source / unsafe template → 400 (reuse the pipeline route's guards verbatim).
- When `after_import` is a valid name, default it from the workspace's `pipeline.default_strategy` when the key is omitted (`get_effective_config`).

**Step 2:** Run — FAIL.

**Step 3:** Implement `POST /api/jobs/import-photos`: validations, then `runner.start("import", work, config=job_config, workspace_id=active_ws)` where `work` calls `run_import_job`. Do not reuse or rename the existing `POST /api/jobs/import` Lightroom route; PR 3 can update the photo Import page to call the new endpoint. No pipeline slot involvement — imports are I/O-bound and must not queue behind a GPU run (that coupling is exactly what the split removes). Record `after_import` in `job_config`.

**Step 4:** Run — PASS. **Step 5: Commit** — `feat: add photo import job endpoint`

### Task 2.5: Working copies read from the card

**Files:**
- Modify: `vireo/import_job.py`
- Test: `vireo/tests/test_import_job.py`

**Step 1: Failing test** — monkeypatch `image_loader.extract_working_copy` to record `(source_path, output_path)` and return True. Import a fixture card containing a RAW-extension file (a tiny JPEG renamed `.NEF` is fine — extraction is stubbed). Assert: extraction was called with the **card** path (or the card companion JPEG for a RAW+JPEG pair), not the archive path; the photo row's `working_copy_path` points at `<vireo_dir>/working/<photo_id>.jpg`; a photo whose extraction fails is left with `working_copy_path` NULL (so the scanner's archive-side backfill retries later) and the import still succeeds.

**Step 2:** Run — FAIL.

**Step 3:** Implement: after each batch's scan + hash stamping, for each new RAW photo id: pick the card-side source (companion JPEG if the pair exists on the card, mirroring `_extract_working_copies`' preference), call `extract_working_copy(card_source, wc_path, max_size=cfg working_copy_max_size, quality=92)`, set `working_copy_path` on success. Pass `skip_paths`/ordering so `scan()`'s own extraction doesn't run first — verify in Task 2.0 whether `scan(vireo_dir=None)` skips extraction entirely (it should — extraction hangs off `vireo_dir`); if so, call scan with `vireo_dir=None` and do both working copies and cache invalidation in the import job's own pass, documenting why.

**Step 4:** Run — PASS. **Step 5: Commit** — `feat: extract working copies from the card during import`

### Task 2.6: Interruption + resume contract

**Files:**
- Test: `vireo/tests/test_import_job.py`

These tests prove `_deindex_staging` has no equivalent here — the whole point of the new shape.

**Step 1:** Cancel mid-run: FakeRunner with `cancelled_ids` set after the first batch (flip it from a `photo_callback`/progress hook). Assert: the job stops at the next checkpoint; every photo row in the catalog corresponds to a verified on-disk file; **no rows were deleted**; the result reports partial counts and `safe_to_format is False`.

**Step 2:** Resume: run `run_import_job` again with the same params over the same card. Assert: previously-landed files are skipped as duplicates (hash- or key-backed against the now-cataloged archive copies), the remainder is copied, and the second result's `copied + skipped_duplicate == discovered` with `safe_to_format True`. Combined catalog = exactly one row per card file.

**Step 3:** Crash-shaped variant: simulate a copy that succeeded but the process died before that batch's scan (copy files for batch 2 manually, no catalog rows) — re-run and assert the importer treats the byte-identical destination file as already-present (no numeric-suffix duplicate!) and catalogs it. This pins the collision-handling path from Task 2.2 against the design's "rescan self-heals" story.

**Step 4: Commit** — `test: import cancel/resume leaves a valid catalog and resumes cleanly`

### Task 2.7: Remote (SSH) archive destination

**Files:**
- Modify: `vireo/import_job.py`, `vireo/app.py`
- Test: `vireo/tests/test_import_job.py` (reuse the fake-rsync harness from `vireo/tests/test_move_remote.py`)

Scope check with the reviewer/user first if this balloons: it is separable into its own PR without weakening 2.1–2.6.

**Step 1: Failing tests** — remote target + subpath resolve via `resolve_remote_archive`'s mapping (rsync to `remote_path/subpath`, catalog at `mount_path/subpath`); per-batch rsync invocation captured by the fake harness; catalog rows point at mount paths; verification is rsync's own transfer checking plus a `--checksum` dry-run (`_remote_verify_complete`) **only when `verify_by_hash`** — resolving the design doc's open question as: size+transfer-integrity by default, full checksum opt-in (it reads every NAS byte; that cost is the user's call, same knob as local).

**Step 2–4:** standard loop. Mount-path catalog rows get `hash_status='verified'` only on the checksum path; otherwise `hash_status='transferred'` — the safe-to-format pill requires `verified` or a hash-backed duplicate, so remote imports without checksum verification honestly report `safe_to_format False` with reason "enable verify_by_hash for remote verification".

**Step 5: Commit** — `feat: remote archive destinations for the import job`

### Task 2.8: Full suite + PR

Run the test command. `gh pr create --base main` titled "Import job: direct-to-archive copy, hash verify, incremental catalog (import/process split PR 2)", referencing #1101/#1102/#1103. Push review fixes to the same branch.

---

## Execution notes

- API-only PR: no template/UI changes; the Import page, "After import" menu, and chaining hook are PR 3.
- `ingest()` and the pipeline's staging path are untouched — they keep working until PR 4 deletes them.
- Batches are the commit unit everywhere: cancel checks, scan calls, hash stamping, and progress all align on batch boundaries so every stopping point is a valid catalog state.
