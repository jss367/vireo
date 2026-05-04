# Pipeline Pills PR #3 — Per-stage outdated flags Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Wire `detail.fingerprint_outdated` into `_eye_keypoints_plan` / `_extract_plan` / `_classify_plan` so PR #748's "Outdated" pill + amber bar fire when settings have changed since the last run, not just for Group.

**Architecture:** Three new DB helpers (one per stage) count items in scope that were processed under stale settings. Each planner reads its helper, exposes `detail.stale` (int) and sets `detail.fingerprint_outdated = (stale > 0)`. UI in PR #748 already handles the flag. No new endpoints, no schema changes.

**Tech Stack:** Python (SQLite), pytest. Test patterns established in PR #1 / #2 use `_make_db` + `_add_photo_with_detection` fixtures.

**Reference design:** `docs/plans/2026-05-13-pipeline-pills-pr3-staleness-design.md`.

**Key file lines (current state):**
- `_classify_plan`: `vireo/pipeline_plan.py:118-244`
- `_extract_plan`: `vireo/pipeline_plan.py:246-280`
- `_eye_keypoints_plan`: `vireo/pipeline_plan.py:283-330`
- `count_eye_keypoint_eligible`: `vireo/db.py:2855-2884`
- `count_classify_pending_pairs`: `vireo/db.py:2783-2816`
- `count_photos_pending_masks`: `vireo/db.py:2818-2853`
- `find_stale_masks`: `vireo/db.py:4088-4154`
- `_scope_clause`: `vireo/db.py:2735-2749`

**Test command (run after each Python-touching task):**
```bash
python -m pytest vireo/tests/test_pipeline_plan.py vireo/tests/test_db.py -v
```

---

## Task 1 — Eye Keypoints staleness

**Files:**
- Modify: `vireo/db.py` (add helper near `count_eye_keypoint_eligible` at line 2855)
- Modify: `vireo/pipeline_plan.py` (`_eye_keypoints_plan`, lines 283-330)
- Test: `vireo/tests/test_pipeline_plan.py`

### Step 1: Write failing tests for the DB helper

Append to `vireo/tests/test_pipeline_plan.py` after the existing `test_count_eye_keypoint_eligible_*` tests (grep for those to find the right spot):

```python
def test_count_eye_keypoint_stale_zero_when_all_current(tmp_path):
    """No stale photos when every eligible row's eye_kp_fingerprint
    matches the current EYE_KP_FINGERPRINT_VERSION."""
    from labels_fingerprint import TOL_SENTINEL
    from pipeline import EYE_KP_FINGERPRINT_VERSION
    db, folder_id = _make_db(tmp_path)
    pid, did = _add_photo_with_detection(db, folder_id, "a.jpg")
    db.conn.execute(
        "UPDATE photos SET mask_path='/m/a.png', "
        "eye_tenengrad=12.0, eye_kp_fingerprint=? WHERE id=?",
        (EYE_KP_FINGERPRINT_VERSION, pid),
    )
    db.conn.execute(
        "INSERT INTO predictions (detection_id, classifier_model, "
        "labels_fingerprint, species, confidence) VALUES (?, ?, ?, ?, ?)",
        (did, "BioCLIP-2", TOL_SENTINEL, "robin", 0.9),
    )
    db.conn.commit()
    assert db.count_eye_keypoint_stale() == 0


def test_count_eye_keypoint_stale_counts_old_fingerprint(tmp_path):
    """A photo with eye_tenengrad set under an old fingerprint counts
    as stale; the planner will use this to flip Outdated."""
    from labels_fingerprint import TOL_SENTINEL
    db, folder_id = _make_db(tmp_path)
    pid, did = _add_photo_with_detection(db, folder_id, "a.jpg")
    db.conn.execute(
        "UPDATE photos SET mask_path='/m/a.png', "
        "eye_tenengrad=12.0, eye_kp_fingerprint='superanimal-old' WHERE id=?",
        (pid,),
    )
    db.conn.execute(
        "INSERT INTO predictions (detection_id, classifier_model, "
        "labels_fingerprint, species, confidence) VALUES (?, ?, ?, ?, ?)",
        (did, "BioCLIP-2", TOL_SENTINEL, "robin", 0.9),
    )
    db.conn.commit()
    assert db.count_eye_keypoint_stale() == 1


def test_count_eye_keypoint_stale_ignores_never_processed(tmp_path):
    """Photos with eye_tenengrad IS NULL are 'never processed', not stale.
    Stale specifically means 'previously processed under different
    settings that no longer match'."""
    from labels_fingerprint import TOL_SENTINEL
    db, folder_id = _make_db(tmp_path)
    pid, did = _add_photo_with_detection(db, folder_id, "a.jpg")
    db.conn.execute(
        "UPDATE photos SET mask_path='/m/a.png' WHERE id=?", (pid,),
    )
    db.conn.execute(
        "INSERT INTO predictions (detection_id, classifier_model, "
        "labels_fingerprint, species, confidence) VALUES (?, ?, ?, ?, ?)",
        (did, "BioCLIP-2", TOL_SENTINEL, "robin", 0.9),
    )
    db.conn.commit()
    assert db.count_eye_keypoint_stale() == 0
```

### Step 2: Run failing

```bash
python -m pytest vireo/tests/test_pipeline_plan.py -k "count_eye_keypoint_stale" -v
```

Expected: 3 FAIL with `AttributeError: 'Database' object has no attribute 'count_eye_keypoint_stale'`.

### Step 3: Implement the helper

Insert into `vireo/db.py` immediately after `count_eye_keypoint_eligible` (around line 2885):

```python
    def count_eye_keypoint_stale(self, photo_ids=None):
        """Count photos in scope whose eye_tenengrad is set under a
        non-current eye_kp_fingerprint. Mirrors
        ``count_eye_keypoint_eligible``'s join shape (workspace + mask +
        detection + prediction) and adds the staleness predicate.

        A NULL fingerprint on a row with eye_tenengrad set is treated as
        stale — only the migration backfill should produce that state,
        and even there the user is expected to re-run after a model
        change to restamp.
        """
        import config as cfg
        from pipeline import EYE_KP_FINGERPRINT_VERSION
        ws = self._ws_id()
        min_conf = self.get_effective_config(cfg.load()).get(
            "detector_confidence", 0.2,
        )
        scope_sql, scope_params = self._scope_clause(photo_ids)
        row = self.conn.execute(
            f"""SELECT COUNT(DISTINCT p.id) AS n
                FROM photos p
                JOIN workspace_folders wf
                  ON wf.folder_id = p.folder_id AND wf.workspace_id = ?
                JOIN detections d
                  ON d.photo_id = p.id
                 AND d.detector_model != 'full-image'
                 AND d.detector_confidence >= ?
                JOIN predictions pr ON pr.detection_id = d.id
                WHERE p.mask_path IS NOT NULL
                  AND p.eye_tenengrad IS NOT NULL
                  AND (p.eye_kp_fingerprint IS NULL
                       OR p.eye_kp_fingerprint != ?){scope_sql}""",
            (ws, min_conf, EYE_KP_FINGERPRINT_VERSION, *scope_params),
        ).fetchone()
        return row["n"] or 0
```

### Step 4: Run helper tests passing

```bash
python -m pytest vireo/tests/test_pipeline_plan.py -k "count_eye_keypoint_stale" -v
```

Expected: 3 PASS.

### Step 5: Write failing tests for the planner integration

Append to `vireo/tests/test_pipeline_plan.py` after the existing `test_eye_keypoints_plan_*` tests:

```python
def test_eye_keypoints_plan_emits_fingerprint_outdated_when_stale(
    tmp_path, monkeypatch,
):
    """The planner must surface fingerprint_outdated + a stale count when
    any eligible photo has a non-current eye_kp_fingerprint. PR #748's
    pill formatter renders this as 'Outdated (N to redo)' + amber bar."""
    import pipeline as pipeline_mod
    from labels_fingerprint import TOL_SENTINEL
    from pipeline_plan import compute_plan
    db, folder_id = _make_db(tmp_path)
    monkeypatch.setattr(
        pipeline_mod, "eye_keypoint_stage_preflight", lambda config: None,
    )
    pid, did = _add_photo_with_detection(db, folder_id, "a.jpg")
    db.conn.execute(
        "UPDATE photos SET mask_path='/m/a.png', "
        "eye_tenengrad=12.0, eye_kp_fingerprint='superanimal-old' WHERE id=?",
        (pid,),
    )
    db.conn.execute(
        "INSERT INTO predictions (detection_id, classifier_model, "
        "labels_fingerprint, species, confidence) VALUES (?, ?, ?, ?, ?)",
        (did, "BioCLIP-2", TOL_SENTINEL, "robin", 0.9),
    )
    db.conn.commit()
    plan = compute_plan(db, _params(), str(tmp_path / "test.db"))
    detail = plan["stages"]["EyeKeypoints"]["detail"]
    assert detail["stale"] == 1
    assert detail["fingerprint_outdated"] is True


def test_eye_keypoints_plan_no_outdated_flag_when_all_current(
    tmp_path, monkeypatch,
):
    """No stale photos → flag absent (or False), pill stays 'Already done'."""
    import pipeline as pipeline_mod
    from labels_fingerprint import TOL_SENTINEL
    from pipeline import EYE_KP_FINGERPRINT_VERSION
    from pipeline_plan import compute_plan
    db, folder_id = _make_db(tmp_path)
    monkeypatch.setattr(
        pipeline_mod, "eye_keypoint_stage_preflight", lambda config: None,
    )
    pid, did = _add_photo_with_detection(db, folder_id, "a.jpg")
    db.conn.execute(
        "UPDATE photos SET mask_path='/m/a.png', "
        "eye_tenengrad=12.0, eye_kp_fingerprint=? WHERE id=?",
        (EYE_KP_FINGERPRINT_VERSION, pid),
    )
    db.conn.execute(
        "INSERT INTO predictions (detection_id, classifier_model, "
        "labels_fingerprint, species, confidence) VALUES (?, ?, ?, ?, ?)",
        (did, "BioCLIP-2", TOL_SENTINEL, "robin", 0.9),
    )
    db.conn.commit()
    plan = compute_plan(db, _params(), str(tmp_path / "test.db"))
    detail = plan["stages"]["EyeKeypoints"]["detail"]
    assert detail["stale"] == 0
    assert not detail.get("fingerprint_outdated")
```

### Step 6: Run failing

```bash
python -m pytest vireo/tests/test_pipeline_plan.py -k "eye_keypoints_plan_emits_fingerprint_outdated or eye_keypoints_plan_no_outdated_flag" -v
```

Expected: 2 FAIL with `KeyError: 'stale'` or `AssertionError`.

### Step 7: Wire helper into `_eye_keypoints_plan`

Modify `vireo/pipeline_plan.py:298-329` (the area between `pending = ...` and the three return statements). In each return path that has a `detail` dict (will-run/eligible=0, done-prior, will-run general case), add:

```python
        "stale": stale,
        "fingerprint_outdated": stale > 0,
```

Computation goes once near the top of the function body (after the `pending` and `eligible` lines, around line 300):

```python
    pending = len(db.list_photos_for_eye_keypoint_stage(photo_ids=photo_ids))
    eligible = db.count_eye_keypoint_eligible(photo_ids)
    stale = db.count_eye_keypoint_stale(photo_ids)
    processed = max(eligible - pending, 0)
```

Then thread `stale` into all three `detail` blocks. The `eligible == 0` branch can use `stale=0` since no eligible photos means nothing to stale-check, but for symmetry pass `stale` anyway (it'll naturally be 0).

### Step 8: Run tests passing

```bash
python -m pytest vireo/tests/test_pipeline_plan.py -k "eye_keypoint" -v
```

Expected: existing + new tests all PASS.

### Step 9: Commit

```bash
git add vireo/db.py vireo/pipeline_plan.py vireo/tests/test_pipeline_plan.py
git commit -m "pipeline-plan: emit fingerprint_outdated for Eye Keypoints stage

Adds db.count_eye_keypoint_stale() and threads it into _eye_keypoints_plan
as detail.stale + detail.fingerprint_outdated. Together with PR #748's UI
formatter, a settings-changed eye-keypoint stage now renders as 'Outdated
(N to redo)' with amber bar, instead of generic 'Resume (N left)' + green.

Stale = eye_tenengrad set under a fingerprint that doesn't match
EYE_KP_FINGERPRINT_VERSION. Photos never processed (eye_tenengrad NULL)
are pending but not stale — they always count toward 'Resume', never
toward 'Outdated'."
```

---

## Task 2 — Extract staleness

**Files:**
- Modify: `vireo/db.py` (add helper near `count_photos_pending_masks` at line 2818)
- Modify: `vireo/pipeline_plan.py` (`_extract_plan`, lines 246-280)
- Test: `vireo/tests/test_pipeline_plan.py`

### Step 1: Write failing tests for the DB helper

```python
def test_count_extract_stale_zero_when_no_masks(tmp_path):
    db, folder_id = _make_db(tmp_path)
    _add_photo_with_detection(db, folder_id, "a.jpg")
    assert db.count_extract_stale("sam2-small") == 0


def test_count_extract_stale_zero_when_prompt_matches(tmp_path):
    """A mask whose stored prompt matches the photo's primary detection
    is fresh — not stale."""
    db, folder_id = _make_db(tmp_path)
    pid, _ = _add_photo_with_detection(db, folder_id, "a.jpg")
    # Stored prompt matches the detection's box (0.1, 0.1, 0.5, 0.5)
    db.conn.execute(
        "INSERT INTO photo_masks(photo_id, variant, path, created_at, "
        "detector_model, prompt_x, prompt_y, prompt_w, prompt_h) "
        "VALUES (?, 'sam2-small', '/m/a.png', 0, 'megadetector-v6', "
        "0.1, 0.1, 0.5, 0.5)",
        (pid,),
    )
    db.conn.commit()
    assert db.count_extract_stale("sam2-small") == 0


def test_count_extract_stale_counts_prompt_mismatch(tmp_path):
    """A mask whose stored prompt does NOT match the photo's primary
    detection (e.g., re-detection produced a different bbox) is stale."""
    db, folder_id = _make_db(tmp_path)
    pid, _ = _add_photo_with_detection(db, folder_id, "a.jpg")
    db.conn.execute(
        "INSERT INTO photo_masks(photo_id, variant, path, created_at, "
        "detector_model, prompt_x, prompt_y, prompt_w, prompt_h) "
        "VALUES (?, 'sam2-small', '/m/a.png', 0, 'megadetector-v6', "
        "0.9, 0.9, 0.05, 0.05)",  # bbox mismatches detection (0.1,0.1,0.5,0.5)
        (pid,),
    )
    db.conn.commit()
    assert db.count_extract_stale("sam2-small") == 1


def test_count_extract_stale_filters_by_variant(tmp_path):
    """Stale masks for a different variant don't count toward the
    configured variant's staleness."""
    db, folder_id = _make_db(tmp_path)
    pid, _ = _add_photo_with_detection(db, folder_id, "a.jpg")
    db.conn.execute(
        "INSERT INTO photo_masks(photo_id, variant, path, created_at, "
        "detector_model, prompt_x, prompt_y, prompt_w, prompt_h) "
        "VALUES (?, 'sam2-large', '/m/a.png', 0, 'megadetector-v6', "
        "0.9, 0.9, 0.05, 0.05)",
        (pid,),
    )
    db.conn.commit()
    assert db.count_extract_stale("sam2-small") == 0
    assert db.count_extract_stale("sam2-large") == 1
```

### Step 2: Run failing

```bash
python -m pytest vireo/tests/test_pipeline_plan.py -k "count_extract_stale" -v
```

Expected: 4 FAIL with `AttributeError`.

### Step 3: Implement the helper

Insert into `vireo/db.py` immediately after `count_photos_pending_masks` (around line 2854):

```python
    def count_extract_stale(self, sam2_variant, photo_ids=None,
                             detector_confidence=None):
        """Count photos in scope that have a photo_masks row for
        ``sam2_variant`` whose stored prompt no longer matches the
        photo's primary detection.

        Reuses the staleness predicate from ``find_stale_masks`` — a
        mask is fresh only when its stored ``(detector_model,
        prompt_xywh)`` equals the highest-confidence non-full-image
        detection on the same photo (with optional ``detector_confidence``
        floor). Filtered by ``sam2_variant`` so a stale mask under a
        different variant doesn't pollute the count for the currently
        configured variant.
        """
        import config as cfg
        ws = self._ws_id()
        if detector_confidence is None:
            detector_confidence = self.get_effective_config(cfg.load()).get(
                "detector_confidence", 0.2,
            )
        scope_sql, scope_params = self._scope_clause(photo_ids)
        row = self.conn.execute(
            f"""SELECT COUNT(DISTINCT pm.photo_id) AS n
                FROM photo_masks pm
                JOIN photos p ON p.id = pm.photo_id
                JOIN workspace_folders wf
                  ON wf.folder_id = p.folder_id AND wf.workspace_id = ?
               WHERE pm.variant = ?
                 AND NOT EXISTS (
                    SELECT 1 FROM detections d
                     WHERE d.id = (
                           SELECT d2.id
                             FROM detections d2
                            WHERE d2.photo_id = pm.photo_id
                              AND d2.detector_model != 'full-image'
                              AND d2.detector_confidence >= ?
                            ORDER BY d2.detector_confidence DESC, d2.id ASC
                            LIMIT 1
                       )
                       AND d.detector_model = pm.detector_model
                       AND d.box_x = pm.prompt_x
                       AND d.box_y = pm.prompt_y
                       AND d.box_w = pm.prompt_w
                       AND d.box_h = pm.prompt_h
                 ){scope_sql}""",
            (ws, sam2_variant, detector_confidence, *scope_params),
        ).fetchone()
        return row["n"] or 0
```

### Step 4: Run helper tests passing

```bash
python -m pytest vireo/tests/test_pipeline_plan.py -k "count_extract_stale" -v
```

Expected: 4 PASS.

### Step 5: Write failing planner test

```python
def test_extract_plan_emits_fingerprint_outdated_when_stale(tmp_path):
    """When the configured sam2_variant has stale masks (prompt mismatch),
    surface fingerprint_outdated + stale count so the UI shows Outdated."""
    from pipeline_plan import compute_plan
    db, folder_id = _make_db(tmp_path)
    pid, _ = _add_photo_with_detection(db, folder_id, "a.jpg")
    # Mask under default sam2_variant ('sam2-small') with mismatched prompt
    db.conn.execute(
        "INSERT INTO photo_masks(photo_id, variant, path, created_at, "
        "detector_model, prompt_x, prompt_y, prompt_w, prompt_h) "
        "VALUES (?, 'sam2-small', '/m/a.png', 0, 'megadetector-v6', "
        "0.9, 0.9, 0.05, 0.05)",
        (pid,),
    )
    db.conn.commit()
    plan = compute_plan(db, _params(), str(tmp_path / "test.db"))
    detail = plan["stages"]["Extract"]["detail"]
    assert detail["stale"] == 1
    assert detail["fingerprint_outdated"] is True
```

### Step 6: Run failing

Same `pytest -k "extract_plan_emits"` — Expected: FAIL.

### Step 7: Wire helper into `_extract_plan`

Modify `_extract_plan` in `vireo/pipeline_plan.py`. The current function takes `(db, params, photo_ids)` and doesn't have access to `pipeline_cfg` for the `sam2_variant`. Two options:

- **(a)** Pass `pipeline_cfg` as a new parameter (mirror `_eye_keypoints_plan`'s signature).
- **(b)** Read `effective_cfg` inside `_extract_plan` via `db.get_effective_config(cfg.load())`.

Use **(a)** — matches the existing pattern. Update the signature to `def _extract_plan(db, params, photo_ids, pipeline_cfg):` and the call site in `compute_plan` (around `_extract_plan(db, params, photo_ids)`).

Then add staleness logic:

```python
def _extract_plan(db, params, photo_ids, pipeline_cfg):
    if params.skip_extract_masks:
        return {
            "state": "will-skip",
            "summary": "Disabled — stage will be skipped",
        }
    counts = db.count_photos_pending_masks(photo_ids)
    eligible = counts["eligible"]
    pending = counts["pending"]
    sam2_variant = pipeline_cfg.get("sam2_variant", "sam2-small")
    stale = db.count_extract_stale(sam2_variant, photo_ids)
    if eligible == 0:
        return {
            "state": "will-run",
            "summary": (
                "Will run after classify produces detections "
                "(no eligible photos yet)"
            ),
            "detail": {"eligible": 0, "pending": 0,
                       "stale": 0, "fingerprint_outdated": False},
        }
    if pending == 0 and stale == 0:
        return {
            "state": "done-prior",
            "summary": (
                f"Masks present for all {eligible} eligible "
                f"photo{_plural(eligible)}"
            ),
            "detail": {"eligible": eligible, "pending": 0,
                       "stale": 0, "fingerprint_outdated": False},
        }
    return {
        "state": "will-run",
        "summary": (
            f"Will extract masks for {pending} "
            f"photo{_plural(pending)} ({eligible} eligible)"
        ),
        "detail": {"eligible": eligible, "pending": pending,
                   "stale": stale, "fingerprint_outdated": stale > 0},
    }
```

Also update the call site in `compute_plan`:

```python
extract = _extract_plan(db, params, photo_ids, pipeline_cfg)
```

### Step 8: Run tests passing

```bash
python -m pytest vireo/tests/test_pipeline_plan.py -k "extract" -v
```

Expected: existing + new tests PASS.

### Step 9: Commit

```bash
git add vireo/db.py vireo/pipeline_plan.py vireo/tests/test_pipeline_plan.py
git commit -m "pipeline-plan: emit fingerprint_outdated for Extract stage

Adds db.count_extract_stale(sam2_variant, ...) using find_stale_masks's
prompt-match logic, and threads it into _extract_plan as detail.stale +
detail.fingerprint_outdated. Filtered by the workspace's configured
sam2_variant so a stale mask under a different variant doesn't poison
the active variant's pill state.

Variants whose stored prompt no longer matches the photo's primary
detection (e.g., user raised detector_confidence or re-ran the detector
under a different model) flip to 'Outdated' instead of 'Will run' or
'Already done'. PR #748's UI handles the rendering."
```

---

## Task 3 — Classify staleness

**Files:**
- Modify: `vireo/db.py` (add helper near `count_classify_pending_pairs` at line 2783)
- Modify: `vireo/pipeline_plan.py` (`_classify_plan`, lines 118-244)
- Test: `vireo/tests/test_pipeline_plan.py`

### Step 1: Write failing tests for the DB helper

```python
def test_count_classify_stale_zero_when_no_runs(tmp_path):
    from labels_fingerprint import TOL_SENTINEL
    db, folder_id = _make_db(tmp_path)
    _add_photo_with_detection(db, folder_id, "a.jpg")
    assert db.count_classify_stale("BioCLIP-2", TOL_SENTINEL) == 0


def test_count_classify_stale_zero_when_current_run_present(tmp_path):
    """A detection with a row matching current (model, fp) is done,
    not stale — even if older rows under different fingerprints exist."""
    from labels_fingerprint import TOL_SENTINEL
    db, folder_id = _make_db(tmp_path)
    _, did = _add_photo_with_detection(db, folder_id, "a.jpg")
    db.record_classifier_run(did, "BioCLIP-2", "fp_old", prediction_count=1)
    db.record_classifier_run(did, "BioCLIP-2", TOL_SENTINEL, prediction_count=1)
    assert db.count_classify_stale("BioCLIP-2", TOL_SENTINEL) == 0


def test_count_classify_stale_counts_old_only_runs(tmp_path):
    """A detection with a row for current model under a stale fingerprint
    AND no row matching current fingerprint is stale."""
    from labels_fingerprint import TOL_SENTINEL
    db, folder_id = _make_db(tmp_path)
    _, did = _add_photo_with_detection(db, folder_id, "a.jpg")
    db.record_classifier_run(did, "BioCLIP-2", "fp_old", prediction_count=1)
    assert db.count_classify_stale("BioCLIP-2", TOL_SENTINEL) == 1
```

### Step 2: Run failing

```bash
python -m pytest vireo/tests/test_pipeline_plan.py -k "count_classify_stale" -v
```

Expected: 3 FAIL with `AttributeError`.

### Step 3: Implement the helper

Insert into `vireo/db.py` immediately after `count_classify_pending_pairs` (around line 2817):

```python
    def count_classify_stale(
        self, classifier_model, labels_fingerprint,
        photo_ids=None, min_conf=None,
    ):
        """Count detections in scope that have a stale classifier_runs row
        for ``classifier_model`` (some non-current fingerprint) AND no row
        matching the current ``labels_fingerprint``.

        A detection with a current-fp row is "done" (not stale). A
        detection with no row at all is "never processed" (counted by
        :meth:`count_classify_pending_pairs`, not here). The stale set is
        their disjoint complement: previously processed under settings
        that no longer match.
        """
        ws = self._ws_id()
        if min_conf is None:
            import config as cfg
            min_conf = self.get_effective_config(cfg.load()).get(
                "detector_confidence", 0.2,
            )
        scope_sql, scope_params = self._scope_clause(photo_ids)
        row = self.conn.execute(
            f"""SELECT COUNT(DISTINCT d.id) AS n
                FROM detections d
                JOIN photos p ON p.id = d.photo_id
                JOIN workspace_folders wf
                  ON wf.folder_id = p.folder_id AND wf.workspace_id = ?
               WHERE d.detector_model != 'full-image'
                 AND d.detector_confidence >= ?
                 AND EXISTS (
                    SELECT 1 FROM classifier_runs cr_stale
                     WHERE cr_stale.detection_id = d.id
                       AND cr_stale.classifier_model = ?
                       AND cr_stale.labels_fingerprint != ?
                 )
                 AND NOT EXISTS (
                    SELECT 1 FROM classifier_runs cr_cur
                     WHERE cr_cur.detection_id = d.id
                       AND cr_cur.classifier_model = ?
                       AND cr_cur.labels_fingerprint = ?
                 ){scope_sql}""",
            (ws, min_conf, classifier_model, labels_fingerprint,
             classifier_model, labels_fingerprint, *scope_params),
        ).fetchone()
        return row["n"] or 0
```

### Step 4: Run helper tests passing

Expected: 3 PASS.

### Step 5: Write failing planner tests

```python
def test_classify_plan_emits_fingerprint_outdated_when_stale(
    tmp_path, monkeypatch,
):
    """A detection classified under fp_old + no current-fp row → outdated."""
    from labels_fingerprint import TOL_SENTINEL
    from pipeline_plan import compute_plan
    db, folder_id = _make_db(tmp_path)
    _, did = _add_photo_with_detection(db, folder_id, "a.jpg")
    db.record_classifier_run(did, "BioCLIP-2", "fp_old", prediction_count=1)

    import labels as labels_mod
    import models as models_mod
    monkeypatch.setattr(models_mod, "get_models", lambda: [
        {"id": "m1", "name": "BioCLIP-2",
         "model_str": "hf-hub:imageomics/bioclip-2",
         "model_type": "bioclip", "downloaded": True},
    ])
    monkeypatch.setattr(labels_mod, "get_active_labels", lambda: [])
    monkeypatch.setattr(labels_mod, "get_saved_labels", lambda: [])

    plan = compute_plan(db, _params(model_ids=["m1"]), str(tmp_path / "test.db"))
    detail = plan["stages"]["Classify"]["detail"]
    assert detail["stale"] == 1
    assert detail["fingerprint_outdated"] is True


def test_classify_plan_no_outdated_flag_when_current(
    tmp_path, monkeypatch,
):
    from labels_fingerprint import TOL_SENTINEL
    from pipeline_plan import compute_plan
    db, folder_id = _make_db(tmp_path)
    _, did = _add_photo_with_detection(db, folder_id, "a.jpg")
    db.record_classifier_run(did, "BioCLIP-2", TOL_SENTINEL, prediction_count=1)

    import labels as labels_mod
    import models as models_mod
    monkeypatch.setattr(models_mod, "get_models", lambda: [
        {"id": "m1", "name": "BioCLIP-2",
         "model_str": "hf-hub:imageomics/bioclip-2",
         "model_type": "bioclip", "downloaded": True},
    ])
    monkeypatch.setattr(labels_mod, "get_active_labels", lambda: [])
    monkeypatch.setattr(labels_mod, "get_saved_labels", lambda: [])

    plan = compute_plan(db, _params(model_ids=["m1"]), str(tmp_path / "test.db"))
    detail = plan["stages"]["Classify"]["detail"]
    assert detail["stale"] == 0
    assert not detail.get("fingerprint_outdated")


def test_classify_plan_reclassify_suppresses_outdated(
    tmp_path, monkeypatch,
):
    """Reclassify is a user override, not a settings-change signal —
    don't render as 'Outdated' even though all pairs will redo."""
    from pipeline_plan import compute_plan
    db, folder_id = _make_db(tmp_path)
    _, did = _add_photo_with_detection(db, folder_id, "a.jpg")
    db.record_classifier_run(did, "BioCLIP-2", "fp_old", prediction_count=1)

    import models as models_mod
    monkeypatch.setattr(models_mod, "get_models", lambda: [
        {"id": "m1", "name": "BioCLIP-2",
         "model_str": "hf-hub:imageomics/bioclip-2",
         "model_type": "bioclip", "downloaded": True},
    ])

    plan = compute_plan(
        db, _params(model_ids=["m1"], reclassify=True),
        str(tmp_path / "test.db"),
    )
    detail = plan["stages"]["Classify"]["detail"]
    assert not detail.get("fingerprint_outdated"), (
        "reclassify is user-explicit; outdated flag should stay off so "
        "pill says 'Re-classify' not 'Outdated'"
    )
```

### Step 6: Run failing

Expected: 3 FAIL.

### Step 7: Wire helper into `_classify_plan`

Modify `vireo/pipeline_plan.py:118-244`. Add staleness computation early in the function (after `det_counts` is fetched) and thread it through every return path's detail dict.

The cleanest spot is right after `unblocked_count` and `eligible` are computed (around line 144). Sum across unblocked models:

```python
    stale_total = 0
    if total_dets > 0 and not params.reclassify:
        for m in models:
            info = label_resolution[m["id"]]
            if info.get("blocked"):
                continue
            fp = info["fingerprint"]
            stale_total += db.count_classify_stale(
                classifier_model=m["name"],
                labels_fingerprint=fp,
                photo_ids=photo_ids,
            )
    # Reclassify is a user override, not a settings-change signal.
    fingerprint_outdated = stale_total > 0 and not params.reclassify
```

Then in every return path that has a `detail` dict, add:

```python
        "stale": stale_total,
        "fingerprint_outdated": fingerprint_outdated,
```

For the early `will-skip` returns (skip_classify, no models), pass `0`/`False`.

### Step 8: Run tests passing

```bash
python -m pytest vireo/tests/test_pipeline_plan.py -k "classify" -v
```

Expected: existing + new tests PASS.

### Step 9: Commit

```bash
git add vireo/db.py vireo/pipeline_plan.py vireo/tests/test_pipeline_plan.py
git commit -m "pipeline-plan: emit fingerprint_outdated for Classify stage

Adds db.count_classify_stale(model, fp, ...) — distinct detections with
a non-current classifier_runs row for the model AND no row matching
current. Threaded into _classify_plan as detail.stale +
detail.fingerprint_outdated, summed across unblocked models.

Reclassify is suppressed: a user override that forces redo isn't a
settings-change signal, so the pill stays 'Re-classify (N pairs)'
instead of flipping to 'Outdated'.

Together with PR #748's UI, switching the active label set or model
flips the Classify pill to 'Outdated (N to redo)' + amber bar."
```

---

## Task 4 — Browser visual smoke (driven by lead, not subagent)

**This task is driven by the human lead, not a subagent.** It needs an isolated dev server, DB seeding, and screenshot capture for the PR description. The lead handles it directly using the same Playwright pattern from PR #748.

Setup:
```bash
mkdir -p /tmp/vireo-pr3-test /tmp/pr3-screenshots
HOME=/tmp/vireo-pr3-test python vireo/app.py --db /tmp/vireo-pr3-test/vireo.db --port 8090 &
```

For each scenario, seed via Python on `/tmp/vireo-pr3-test/vireo.db`, drive Playwright at `http://localhost:8090/pipeline`, capture screenshot:

1. **Eye Keypoints outdated** — seed eligible photos with `eye_kp_fingerprint='superanimal-old'`. Reload → Eye Keypoints pill = "Outdated (N to redo)" + amber bar.
2. **Extract outdated** — seed photo_masks with mismatched prompt. Reload → Extract pill = "Outdated (N to redo)" + amber bar.
3. **Classify outdated** — seed `classifier_runs` under `fp_old` with no current-fp row. Reload → Classify pill = "Outdated (N to redo)" + amber bar.

Capture full-page screenshots into `/tmp/pr3-screenshots/`. They go into the PR description.

---

## Task 5 — Push + open PR

**Step 1: Run the project's focused test command**

```bash
python -m pytest vireo/tests/test_pipeline_plan.py vireo/tests/test_db.py vireo/tests/test_app.py vireo/tests/test_pipeline.py vireo/tests/test_pipeline_job.py
```

Triage: 2 pre-existing keyword failures from `MEMORY.md` are OK; anything else is on us.

**Step 2: Push and open PR**

```bash
git push -u origin claude/per-stage-outdated
gh pr create --base main --title "pipeline-plan: per-stage outdated flags for Classify, Extract, Eye Keypoints" --body "$(cat <<'EOF'
## Summary

PR #3 of the pipeline status makeover ([design](docs/plans/2026-05-13-pipeline-pills-pr3-staleness-design.md)). Backend-only.

PR #748 wired the pill formatter and progress bar to render an "Outdated" / amber state when a stage's plan entry sets `detail.fingerprint_outdated`. Today only `_regroup_plan` emits it. This PR brings Classify, Extract, and Eye Keypoints up to the same contract — the visual feature is now reachable for the most common settings-changed cases.

Each stage adds:
- A new \`db.count_*_stale(...)\` helper that counts items in scope previously processed under settings that no longer match.
- \`detail.stale\` (int) and \`detail.fingerprint_outdated = (stale > 0)\` on the planner's return dict.

Reclassify on Classify is a user-explicit override, so it suppresses the outdated flag — the pill stays "Re-classify (N pairs)" rather than flipping to "Outdated".

## Test plan
- [x] 3 db helpers, 10 new tests covering helper + planner (per stage).
- [x] Existing \`test_pipeline_plan.py\` tests stay green.
- [x] Browser smoke via Playwright on isolated dev server: 3 scenarios captured (Eye Keypoints / Extract / Classify Outdated). Screenshots inline.

[Screenshots from /tmp/pr3-screenshots/]

🤖 Generated with [Claude Code](https://claude.com/claude-code)
EOF
)"
```

---

## Out of scope

- Live SSE counts in pills during runs.
- Provenance line per card.
- Removing legacy `has_*` consumers from `/api/pipeline/page-init`.

These remain as future polish; this PR delivers the headline staleness-visibility UX.
