# Pipeline Classify Counter Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Split the classify stage's progress counter into "inferred" vs "cached" and add a one-shot pre-flight estimate so the user sees honest progress and ETA from the first second.

**Architecture:** Add an optional `cached` integer (incremented on cache-hit) and a `cached_estimate` integer (set once at stage start) to the per-stage progress payload. Backend populates them in `vireo/pipeline_job.py`'s classify loop. UI in `vireo/templates/pipeline.html` renders the new fields when present, falling back to the existing single-number display when absent. New DB method `count_classifier_runs` provides the pre-flight count.

**Tech Stack:** Python 3.10+, SQLite via `sqlite3`, Flask + Jinja2, vanilla JS.

**Design doc:** see the matching `*-design.md` in this directory (cherry-picked from `current-job-status` branch where it was authored, commit b0ee9a6).

**Working directory:** `/Users/julius/git/vireo/.worktrees/classify-counter` (branch `claude/classify-counter`, branched from `origin/main`).

**Test command (full suite per CLAUDE.md):**
```bash
python -m pytest tests/test_workspaces.py vireo/tests/test_db.py vireo/tests/test_app.py vireo/tests/test_photos_api.py vireo/tests/test_edits_api.py vireo/tests/test_jobs_api.py vireo/tests/test_darktable_api.py vireo/tests/test_config.py vireo/tests/test_pipeline_job.py -v
```

For incremental TDD use the targeted commands inside each task.

---

## Task 1: Add `count_classifier_runs` DB method (TDD)

**Files:**
- Modify: `vireo/db.py` (add method near `get_classifier_run_keys` at line 5802)
- Modify: `vireo/tests/test_db.py` (append two tests at end)

### Step 1: Verify `add_detection` signature

Before writing the test, run:

```bash
grep -n "def add_detection" vireo/db.py
```

Read the function signature so the test setup matches. Adjust the test calls in Step 2 if the signature differs from `(photo_id, box_x, box_y, box_w, box_h, detector_confidence, category, detector_model)`.

### Step 2: Write the first failing test

Append to `vireo/tests/test_db.py`:

```python
def test_count_classifier_runs_filters_by_model_and_fingerprint(tmp_path):
    """count_classifier_runs returns the number of distinct photos in the
    given id list that have at least one detection with a classifier_runs
    row matching the given (model, fingerprint)."""
    from db import Database
    db = Database(str(tmp_path / "test.db"))
    fid = db.add_folder("/photos", name="photos")
    p1 = db.add_photo(folder_id=fid, filename="a.jpg", extension=".jpg",
                     file_size=1, file_mtime=1.0, timestamp=None,
                     width=1, height=1)
    p2 = db.add_photo(folder_id=fid, filename="b.jpg", extension=".jpg",
                     file_size=1, file_mtime=1.0, timestamp=None,
                     width=1, height=1)
    p3 = db.add_photo(folder_id=fid, filename="c.jpg", extension=".jpg",
                     file_size=1, file_mtime=1.0, timestamp=None,
                     width=1, height=1)

    d1 = db.add_detection(p1, 0.0, 0.0, 1.0, 1.0, 0.9, "animal", "test-det")
    d2 = db.add_detection(p2, 0.0, 0.0, 1.0, 1.0, 0.9, "animal", "test-det")
    d3 = db.add_detection(p3, 0.0, 0.0, 1.0, 1.0, 0.9, "animal", "test-det")

    db.record_classifier_run(d1, "BioCLIP-2.5", "fp-a", prediction_count=1)
    db.record_classifier_run(d2, "BioCLIP-2.5", "fp-b", prediction_count=1)
    db.record_classifier_run(d3, "iNat21", "fp-a", prediction_count=1)

    # Only p1 matches (BioCLIP-2.5 + fp-a).
    assert db.count_classifier_runs(
        [p1, p2, p3], "BioCLIP-2.5", "fp-a"
    ) == 1

    # Empty input returns 0.
    assert db.count_classifier_runs([], "BioCLIP-2.5", "fp-a") == 0

    # Photo with multiple detections, only one cached, still counts as 1.
    d2b = db.add_detection(p2, 0.0, 0.0, 1.0, 1.0, 0.9, "animal", "test-det")
    db.record_classifier_run(d2b, "BioCLIP-2.5", "fp-a", prediction_count=1)
    assert db.count_classifier_runs(
        [p1, p2, p3], "BioCLIP-2.5", "fp-a"
    ) == 2  # p1 and p2
```

### Step 3: Run the test, verify it fails

```bash
cd /Users/julius/git/vireo/.worktrees/classify-counter
python -m pytest vireo/tests/test_db.py::test_count_classifier_runs_filters_by_model_and_fingerprint -v
```

**Expected:** FAIL with `AttributeError: 'Database' object has no attribute 'count_classifier_runs'`.

### Step 4: Implement `count_classifier_runs` in `vireo/db.py`

Insert after `get_classifier_run_keys` (line 5810):

```python
def count_classifier_runs(self, photo_ids, classifier_model, labels_fingerprint):
    """Count distinct photos in `photo_ids` that have at least one
    detection with a classifier_runs row matching the given
    (classifier_model, labels_fingerprint).

    Used by the streaming pipeline's classify stage to pre-flight how
    many photos will hit the cache vs. require fresh inference.
    """
    if not photo_ids:
        return 0
    # Chunk to stay under SQLITE_MAX_VARIABLE_NUMBER (default 999).
    # Match the 500-element chunks used elsewhere in this file.
    CHUNK = 500
    matched = set()
    for i in range(0, len(photo_ids), CHUNK):
        chunk = photo_ids[i:i + CHUNK]
        placeholders = ",".join("?" * len(chunk))
        rows = self.conn.execute(
            f"SELECT DISTINCT d.photo_id "
            f"FROM detections d "
            f"JOIN classifier_runs cr ON cr.detection_id = d.id "
            f"WHERE cr.classifier_model = ? "
            f"  AND cr.labels_fingerprint = ? "
            f"  AND d.photo_id IN ({placeholders})",
            [classifier_model, labels_fingerprint, *chunk],
        ).fetchall()
        for r in rows:
            matched.add(r["photo_id"])
    return len(matched)
```

### Step 5: Run the test, verify it passes

```bash
python -m pytest vireo/tests/test_db.py::test_count_classifier_runs_filters_by_model_and_fingerprint -v
```

**Expected:** PASS.

### Step 6: Write the chunking test

Append to `vireo/tests/test_db.py`:

```python
def test_count_classifier_runs_chunks_large_id_lists(tmp_path):
    """count_classifier_runs returns the correct count even when the
    photo id list exceeds SQLITE_MAX_VARIABLE_NUMBER (default 999)."""
    from db import Database
    db = Database(str(tmp_path / "test.db"))
    fid = db.add_folder("/photos", name="photos")

    # 1500 photos, each with one detection and one classifier_runs row
    # for (BioCLIP-2.5, fp-a). Above the 999 SQLite variable cap.
    photo_ids = []
    for i in range(1500):
        pid = db.add_photo(
            folder_id=fid, filename=f"p_{i:05d}.jpg", extension=".jpg",
            file_size=1, file_mtime=1.0, timestamp=None,
            width=1, height=1,
        )
        did = db.add_detection(pid, 0.0, 0.0, 1.0, 1.0, 0.9, "animal", "test-det")
        db.record_classifier_run(did, "BioCLIP-2.5", "fp-a", prediction_count=1)
        photo_ids.append(pid)

    assert db.count_classifier_runs(
        photo_ids, "BioCLIP-2.5", "fp-a"
    ) == 1500
```

### Step 7: Run the chunking test, verify it passes

```bash
python -m pytest vireo/tests/test_db.py::test_count_classifier_runs_chunks_large_id_lists -v
```

**Expected:** PASS.

### Step 8: Commit

```bash
git add vireo/db.py vireo/tests/test_db.py
git commit -m "$(cat <<'EOF2'
db: add count_classifier_runs for classify pre-flight

Returns the number of distinct photos in a given id list that have at
least one cached classifier_runs row for the active model+fingerprint.
Chunked over SQLITE_MAX_VARIABLE_NUMBER (999) to handle large
collections.

Co-Authored-By: Claude Opus 4.7 <noreply@anthropic.com>
EOF2
)"
```

---

## Task 2: Add pre-flight estimate in pipeline_job classify loop

**Files:**
- Modify: `vireo/pipeline_job.py` (insert at top of per-spec block, ~line 1859)

### Step 1: Insert the pre-flight call

In `vireo/pipeline_job.py`, find the block that starts at line 1859 (`raw_results: list = []`). Just BEFORE that line, and AFTER line 1858's comment block ends, insert:

```python
                # Pre-flight cache estimate. One indexed query per spec
                # so the UI can display "~M cached, ~K to classify" before
                # the first inference runs and ETAs are honest from the
                # start. The estimate may overcount if a run-key exists
                # but no cached predictions do (see lines ~2000-2004); the
                # live `cached` counter reflects actual skips.
                cached_est = thread_db.count_classifier_runs(
                    [p["id"] for p in photos],
                    model_name,
                    spec_fp,
                )
                stages["classify"]["cached_estimate"] = (
                    stages["classify"].get("cached_estimate", 0) + cached_est
                )
                runner.push_event(job["id"], "progress", _progress_event(
                    stages, "classify",
                    f"Classifying with {active_spec['name']}",
                    step_id=step_id,
                ))
```

The placement matters: this must run AFTER `spec_fp` is set (line 1843) and BEFORE the batch loop. Insert it between lines 1858 and 1859.

### Step 2: Run a smoke test to ensure the pipeline still loads

```bash
python -c "import sys; sys.path.insert(0, 'vireo'); import pipeline_job; print('ok')"
```

**Expected:** `ok` printed, no traceback.

### Step 3: Run the existing pipeline_job test suite

```bash
python -m pytest vireo/tests/test_pipeline_job.py -v
```

**Expected:** All existing tests still pass. The new pre-flight query is harmless for existing tests since they all use `skip_classify=True`.

### Step 4: Commit

```bash
git add vireo/pipeline_job.py
git commit -m "$(cat <<'EOF2'
pipeline_job: pre-flight classify cache estimate

Before each model's batch loop, count how many photos already have a
classifier_runs row for the active (model, fingerprint) and surface
the count as stages.classify.cached_estimate. Lets the UI show
"~M cached, ~K to classify" before any inference runs.

Co-Authored-By: Claude Opus 4.7 <noreply@anthropic.com>
EOF2
)"
```

---

## Task 3: Replace per-batch counter pre-advance with per-photo increments

**Files:**
- Modify: `vireo/pipeline_job.py` (classify loop body, lines 1869-1898 and 1944-2076)

### Step 1: Initialize cached counter at stage start

Find line 1861 (`skipped_existing = 0`). Just below it, ensure `stages["classify"]["cached"]` starts at 0 for this spec (cumulative across multi-spec runs). Insert AFTER line 1861:

```python
                stages["classify"].setdefault("cached", 0)
```

### Step 2: Remove per-batch pre-advance

In the for-loop starting at line 1869, locate lines 1882-1898 (the `# Aggregate classify progress across models...` block plus the `runner.push_event(...)` and `runner.update_step(...)` calls).

**Delete lines 1882-1898 entirely.** That block was the per-batch pre-advance. We will push events at batch *end* now, after the inner loop has incremented per-photo counters.

### Step 3: Increment cached counter on cache hit

Find line 1964 (`skipped_existing += 1`, inside the `if cached:` block). Add a sibling line:

```python
                                    stages["classify"]["cached"] += 1
```

Final shape:

```python
                                if cached:
                                    skipped_existing += 1
                                    stages["classify"]["cached"] += 1
                                    top = cached[0]
                                    ...
```

### Step 4: Increment count counter on successful inference

Find lines 2040-2044 (the `if new_count > 0:` block where `record_classifier_run` is called). Add a sibling that increments classify count:

```python
                        new_count = len(raw_results) - pre_len
                        if new_count > 0:
                            thread_db.record_classifier_run(
                                primary_det["id"], model_name, spec_fp,
                                prediction_count=new_count,
                            )
                            stages["classify"]["count"] = (
                                stages["classify"].get("count", 0) + 1
                            )
```

### Step 5: Push progress event at batch end

After the inner `for photo in batch:` loop closes (just before line 2046's `# Skip the grouping/storage finalization on cancel` block), add a batch-boundary event push:

```python
                    # Batch boundary: surface the per-photo accumulated
                    # count + cached to the UI. Replaces the old per-batch
                    # pre-advance which lied about progress when batches
                    # contained cache hits.
                    stages["classify"]["total"] = total * len(resolved_specs_local)
                    elapsed = max(time.time() - start_time, 0.01)
                    seen = stages["classify"]["count"] + stages["classify"]["cached"]
                    runner.push_event(job["id"], "progress", _progress_event(
                        stages, "classify",
                        f"Classifying with {active_spec['name']}"
                        + (
                            f" ({spec_idx + 1}/{len(resolved_specs_local)})"
                            if len(resolved_specs_local) > 1 else ""
                        ),
                        step_id=step_id,
                        rate=round(seen / elapsed * 60, 1),
                    ))
                    runner.update_step(
                        job["id"], step_id,
                        progress={
                            "current": stages["classify"]["count"],
                            "total": total,
                        },
                    )
```

### Step 6: Simplify mid-batch cancel cleanup

Lines 2059-2076 contain the corrective fixup that adjusts `count` after a mid-batch cancel because of the old pre-advance. With per-photo accuracy this fixup is no longer needed — `count` and `cached` are already correct.

Replace the entire block from `if _should_abort(abort):` at line 2052 through line 2076 with:

```python
                # Skip the grouping/storage finalization on cancel — it can
                # take a minute on large collections and the user has already
                # asked us to stop. Per-photo counters are accurate, no
                # corrective fixup needed.
                if _should_abort(abort):
                    runner.update_step(
                        job["id"], step_id,
                        status="completed",
                        progress={
                            "current": stages["classify"]["count"],
                            "total": total,
                        },
                        summary=(
                            f"Cancelled "
                            f"({stages['classify']['count']} classified, "
                            f"{stages['classify']['cached']} cached "
                            f"of {total})"
                        ),
                    )
                    continue
```

### Step 7: Run pipeline_job tests

```bash
python -m pytest vireo/tests/test_pipeline_job.py -v
```

**Expected:** All existing tests pass.

### Step 8: Run full suite per CLAUDE.md

```bash
python -m pytest tests/test_workspaces.py vireo/tests/test_db.py vireo/tests/test_app.py vireo/tests/test_photos_api.py vireo/tests/test_edits_api.py vireo/tests/test_jobs_api.py vireo/tests/test_darktable_api.py vireo/tests/test_config.py vireo/tests/test_pipeline_job.py -v
```

**Expected:** All pass.

### Step 9: Commit

```bash
git add vireo/pipeline_job.py
git commit -m "$(cat <<'EOF2'
pipeline_job: track classify cached vs inferred per photo

Replaces the per-batch counter pre-advance with per-photo increments
so the live progress event distinguishes cache hits (cached) from
fresh inferences (count). Removes the mid-batch cancel fixup since
counters are now accurate at every photo boundary.

stages.classify shape:
  count           - photos that ran inference
  cached          - photos that hit the classifier_runs cache
  total           - photos in the run (x number of specs)
  cached_estimate - pre-flight estimate from prior task

Co-Authored-By: Claude Opus 4.7 <noreply@anthropic.com>
EOF2
)"
```

---

## Task 4: Render `cached` and `cached_estimate` in pipeline.html

**Files:**
- Modify: `vireo/templates/pipeline.html` (function `_updatePipelineStageUI`, around line 2293)

### Step 1: Update the JS render block

In `vireo/templates/pipeline.html`, locate `_updatePipelineStageUI` (line 2293). Find the block at lines 2334-2360. Replace the existing logic with:

```javascript
  var si = stages[p.stage_id] || {};
  var stageCurrent = si.count || 0;
  var stageCached = si.cached || 0;
  var stageCachedEst = si.cached_estimate || 0;
  var stageTotal = si.total || 0;

  var parts = [];
  if (p.phase) parts.push(p.phase);

  // Count line: split when we have actually seen cache hits, otherwise
  // fall back to single-number display so non-classify stages render
  // unchanged.
  if (stageTotal > 0 && (stageCurrent > 0 || stageCached > 0)) {
    if (stageCached > 0) {
      parts.push(
        stageCurrent.toLocaleString() + ' inferred · ' +
        stageCached.toLocaleString() + ' cached / ' +
        stageTotal.toLocaleString()
      );
    } else {
      parts.push(stageCurrent.toLocaleString() + ' / ' + stageTotal.toLocaleString());
    }
  }

  // Pre-flight banner: shown once when the stage starts and we have an
  // estimate but no actual progress yet.
  if (stageCachedEst > 0 && stageCurrent === 0 && stageCached === 0) {
    parts.push(
      '~' + stageCachedEst.toLocaleString() + ' cached, ~' +
      (stageTotal - stageCachedEst).toLocaleString() + ' to classify'
    );
  }

  if (p.rate) parts.push(Math.round(p.rate) + ' files/min');
  if (p.eta_seconds != null && p.eta_seconds > 0) {
    parts.push('~' + _formatETA(p.eta_seconds) + ' remaining');
  } else if (stageTotal > 0 && stageCurrent > 0 && stageCurrent < 10) {
    parts.push('Estimating...');
  }
  if (p.current_file) parts.push(p.current_file);
  var label = parts.join(' — ');

  if (status) status.textContent = label;

  if (stageTotal > 0) {
    if (progressWrap) progressWrap.style.display = '';
    // Bar fill must reflect both inferred AND cached photos so the bar
    // tracks honest completion, not just inferences.
    var pct = Math.round((stageCurrent + stageCached) / stageTotal * 100);
    if (fill) fill.style.width = pct + '%';
```

(Keep whatever lines come after `if (fill) fill.style.width = pct + '%';` unchanged.)

### Step 2: Commit

```bash
git add vireo/templates/pipeline.html
git commit -m "$(cat <<'EOF2'
pipeline UI: render classify cached vs inferred counts

Splits the classify stage's count line into "X inferred . Y cached / N"
when cache hits exist. Shows a pre-flight banner ("~M cached, ~K to
classify") before any photos process. Bar fill reflects (count + cached)
/ total so percentage matches honest completion.

Other stages render unchanged - they don't set cached, so the new
branches are never taken.

Co-Authored-By: Claude Opus 4.7 <noreply@anthropic.com>
EOF2
)"
```

---

## Task 5: Manual browser verification

**Why manual:** The change is primarily UX. Per CLAUDE.md "user-first testing", drive a real browser for UI changes.

### Step 1: Start a Vireo dev instance from the worktree

```bash
cd /Users/julius/git/vireo/.worktrees/classify-counter
python vireo/app.py --db ~/.vireo/vireo-classify-counter-test.db --port 8090 &
```

Open http://127.0.0.1:8090 in a browser.

### Step 2: Set up test data

In the test DB, create a workspace with a small folder (~20 photos). Run a full pipeline once to populate detections + classifier_runs. Stop after classify completes.

### Step 3: Re-run classify on the same collection

This time, every photo should hit the cache. Expected UI behavior:

- Pre-flight banner appears at stage start: `Classifying species - ~20 cached, ~0 to classify`.
- During the run, count line shows: `Classifying species - 0 inferred . 20 cached / 20`.
- Bar fills 0% to 100% smoothly as cached counter ticks.
- Final state: `0 inferred . 20 cached / 20` with bar at 100%.

### Step 4: Add a small new folder, re-run on a mixed collection

Add 5 fresh photos (no detections yet). Run the full pipeline. After detect completes, the classify stage should show:

- Pre-flight: `~20 cached, ~5 to classify`.
- Mid-run: numbers split between cached (climbing fast through old photos) and inferred (climbing slowly through new ones).
- Final: `5 inferred . 20 cached / 25`.

### Step 5: Sanity-check non-classify stages

Confirm thumbnails, previews, detect, regroup all still render their existing single-number progress (no `inferred . cached` split, no banner).

### Step 6: Stop the dev instance

```bash
kill %1
```

### Step 7: Push the branch and open a PR

```bash
git push -u origin claude/classify-counter
gh pr create --base main --title "pipeline: split classify counter into inferred vs cached" \
  --body "$(cat <<'EOF2'
## Summary

Splits the classify stage's progress counter into "inferred" vs "cached" and adds a one-shot pre-flight estimate so users see honest progress and ETA from the first second of the run.

Motivated by a 24,647-photo classify run where ~54% of photos already had cached predictions but the counter advanced for both kinds at the same rate, making a half-cached run look as slow as a full one.

## Changes

- db.count_classifier_runs(photo_ids, model, fingerprint) - pre-flight count.
- pipeline_job.py - per-photo increments (count for inference, cached for skip), pre-flight estimate at stage start, simpler mid-batch cancel cleanup.
- pipeline.html - renders "X inferred . Y cached / N", pre-flight banner, honest bar fill.

## Test plan

- [x] DB tests: count_classifier_runs filters by model+fingerprint, chunks > 999 ids.
- [x] All existing pipeline_job tests still pass.
- [x] Manual: re-run classify on fully-cached collection - banner + counter render correctly.
- [x] Manual: run on mixed cached/uncached collection - both counters climb honestly.
- [x] Manual: other stages (thumbnails, detect, regroup) render unchanged.
EOF2
)"
```

---

## Done

When all five tasks complete and the PR is open, hand control back. Implementation done.
