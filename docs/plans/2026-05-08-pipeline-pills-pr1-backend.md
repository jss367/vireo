# Pipeline Pills PR #1 — Backend Uniformization

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Add uniform `detail.pending` + `detail.eligible` to `_classify_plan` in `vireo/pipeline_plan.py` so the upcoming UI work (PR #2) can render pill counts and progress bars without per-stage count knowledge.

**Architecture:** Single function modification — every return path of `_classify_plan` gains both keys. `_extract_plan` and `_eye_keypoints_plan` already conform; `_regroup_plan` is intentionally skipped (no per-photo unit). For Classify, the unit is detection-model pairs: `eligible = total_dets * len(unblocked_models)`, `pending = pending_pairs` (or `eligible` when `params.reclassify`).

**Tech Stack:** Python, pytest. Existing tests in `vireo/tests/test_pipeline_plan.py` use `_make_db` + `_add_photo_with_detection` fixtures and monkeypatch `models.get_models` / `labels.get_active_labels`.

**Reference design:** `docs/plans/2026-05-08-pipeline-status-makeover-phase2-design.md`.

**Test command:**
```bash
python -m pytest vireo/tests/test_pipeline_plan.py -v
```

---

## Task 1.1: Backfill `pending` + `eligible` on every Classify return path

**Files:**
- Modify: `vireo/pipeline_plan.py:118-221` (`_classify_plan`)
- Test: `vireo/tests/test_pipeline_plan.py`

### Step 1: Read current shape

`_classify_plan` has six return paths:

1. `params.skip_classify` → `will-skip`, no detail (already fine; counts not meaningful for skipped stages but adding 0/0 is harmless)
2. `not models` → `will-skip`, no detail (same)
3. `total_dets == 0` → `will-run`, detail has `total_dets`/`photos_with_dets`/`models`
4. `blocked and not pending_total` → `will-run`, detail has `blocked_models` only
5. `pending_total == 0` → `done-prior`, detail has `total_dets`/`photos_with_dets`/`models`
6. The general case → `will-run`, detail has `pending_pairs`/`per_model`/`total_dets`/`photos_with_dets`/optional `blocked_models`

### Step 2: Write failing tests

Add to `vireo/tests/test_pipeline_plan.py` (after `test_classify_plan_reclassify_bypasses_cache`, around line 290):

```python
def test_classify_plan_exposes_pending_and_eligible_done_prior(tmp_path, monkeypatch):
    """Every classify return path must expose detail.pending + detail.eligible
    so the UI's pill formatter doesn't need per-stage count knowledge.
    Done-prior path: eligible = total_dets * num_models, pending = 0."""
    from labels_fingerprint import TOL_SENTINEL
    from pipeline_plan import compute_plan
    db, folder_id = _make_db(tmp_path)
    _, did = _add_photo_with_detection(db, folder_id, "a.jpg")

    import labels as labels_mod
    import models as models_mod
    monkeypatch.setattr(models_mod, "get_models", lambda: [
        {"id": "m1", "name": "BioCLIP-2",
         "model_str": "hf-hub:imageomics/bioclip-2",
         "model_type": "bioclip", "downloaded": True},
    ])
    monkeypatch.setattr(labels_mod, "get_active_labels", lambda: [])
    monkeypatch.setattr(labels_mod, "get_saved_labels", lambda: [])
    db.record_classifier_run(did, "BioCLIP-2", TOL_SENTINEL, prediction_count=3)

    plan = compute_plan(db, _params(model_ids=["m1"]), str(tmp_path / "test.db"))
    detail = plan["stages"]["Classify"]["detail"]
    assert detail["pending"] == 0
    assert detail["eligible"] == 1  # 1 detection × 1 model


def test_classify_plan_exposes_pending_and_eligible_will_run(tmp_path, monkeypatch):
    """will-run path with new model added: eligible counts pairs across
    all unblocked models, pending counts the unfinished ones."""
    from labels_fingerprint import TOL_SENTINEL
    from pipeline_plan import compute_plan
    db, folder_id = _make_db(tmp_path)
    _, did = _add_photo_with_detection(db, folder_id, "a.jpg")

    import models as models_mod
    monkeypatch.setattr(models_mod, "get_models", lambda: [
        {"id": "m1", "name": "BioCLIP-2",
         "model_str": "hf-hub:imageomics/bioclip-2",
         "model_type": "bioclip", "downloaded": True},
        {"id": "m2", "name": "BioCLIP",
         "model_str": "hf-hub:imageomics/bioclip",
         "model_type": "bioclip", "downloaded": True},
    ])
    db.record_classifier_run(did, "BioCLIP-2", TOL_SENTINEL, prediction_count=3)

    plan = compute_plan(
        db, _params(model_ids=["m1", "m2"]), str(tmp_path / "test.db"),
    )
    detail = plan["stages"]["Classify"]["detail"]
    assert detail["eligible"] == 2  # 1 detection × 2 models
    assert detail["pending"] == 1   # only m2 is unrun


def test_classify_plan_exposes_pending_and_eligible_reclassify(tmp_path, monkeypatch):
    """Reclassify path: pending == eligible (everything will redo)."""
    from labels_fingerprint import TOL_SENTINEL
    from pipeline_plan import compute_plan
    db, folder_id = _make_db(tmp_path)
    _, did = _add_photo_with_detection(db, folder_id, "a.jpg")

    import models as models_mod
    monkeypatch.setattr(models_mod, "get_models", lambda: [
        {"id": "m1", "name": "BioCLIP-2",
         "model_str": "hf-hub:imageomics/bioclip-2",
         "model_type": "bioclip", "downloaded": True},
    ])
    db.record_classifier_run(did, "BioCLIP-2", TOL_SENTINEL, prediction_count=3)

    plan = compute_plan(
        db,
        _params(model_ids=["m1"], reclassify=True),
        str(tmp_path / "test.db"),
    )
    detail = plan["stages"]["Classify"]["detail"]
    assert detail["eligible"] == 1
    assert detail["pending"] == 1  # reclassify forces all pairs to redo


def test_classify_plan_exposes_pending_and_eligible_no_detections(tmp_path, monkeypatch):
    """No detections cached yet: eligible=0, pending=0. Bar will hide."""
    from pipeline_plan import compute_plan
    db, _ = _make_db(tmp_path)

    import models as models_mod
    monkeypatch.setattr(models_mod, "get_models", lambda: [
        {"id": "m1", "name": "BioCLIP-2",
         "model_str": "hf-hub:imageomics/bioclip-2",
         "model_type": "bioclip", "downloaded": True},
    ])

    plan = compute_plan(db, _params(model_ids=["m1"]), str(tmp_path / "test.db"))
    detail = plan["stages"]["Classify"]["detail"]
    assert detail["eligible"] == 0
    assert detail["pending"] == 0


def test_classify_plan_exposes_pending_and_eligible_blocked_only(tmp_path, monkeypatch):
    """Blocked-only path (model needs labels): eligible=0 since no model can run.
    pending=0. UI hides the bar; the summary explains the block."""
    from pipeline_plan import compute_plan
    db, folder_id = _make_db(tmp_path)
    _, did = _add_photo_with_detection(db, folder_id, "a.jpg")

    import labels as labels_mod
    import models as models_mod
    # timm models without labels are blocked. Use a non-bioclip model_str
    # so the TOL fallback doesn't kick in.
    monkeypatch.setattr(models_mod, "get_models", lambda: [
        {"id": "m1", "name": "SomeTimmModel",
         "model_str": "hf-hub:other/model",
         "model_type": "bioclip", "downloaded": True},
    ])
    monkeypatch.setattr(labels_mod, "get_active_labels", lambda: [])
    monkeypatch.setattr(labels_mod, "get_saved_labels", lambda: [])

    plan = compute_plan(db, _params(model_ids=["m1"]), str(tmp_path / "test.db"))
    detail = plan["stages"]["Classify"]["detail"]
    assert detail["eligible"] == 0  # no unblocked models
    assert detail["pending"] == 0
```

### Step 3: Run failing

```bash
python -m pytest vireo/tests/test_pipeline_plan.py -k "exposes_pending_and_eligible" -v
```

Expected: 5 FAIL with `KeyError: 'pending'` or `KeyError: 'eligible'`.

### Step 4: Implement

Open `vireo/pipeline_plan.py:118-221` (`_classify_plan`). Add `pending` + `eligible` to every detail dict. The cleanest factoring is to compute `unblocked_count` once and add the two keys at each return site.

The computation:
- `unblocked_count` = number of models without `info.get("blocked")` flag.
- `eligible = total_dets * unblocked_count` (0 when no models, no detections, or all blocked).
- `pending` per branch:
  - `total_dets == 0` → 0
  - blocked-only → 0 (eligible already 0)
  - `done-prior` → 0
  - reclassify general case → `eligible`
  - non-reclassify general case → `pending_total`

Apply the changes directly:

```python
def _classify_plan(db, params, photo_ids):
    if params.skip_classify:
        return {
            "state": "will-skip",
            "summary": "Disabled — stage will be skipped",
            "detail": {"pending": 0, "eligible": 0},
        }

    models = _resolve_models(params.model_ids)
    if not models:
        return {
            "state": "will-skip",
            "summary": "No models selected — stage will be skipped",
            "detail": {"pending": 0, "eligible": 0},
        }

    label_resolution = _resolve_labels_for_models(models, params.labels_files, db)

    det_counts = db.count_real_detections_in_scope(photo_ids)
    total_dets = det_counts["total_dets"]
    photos_with_dets = det_counts["photos_with_dets"]

    unblocked_count = sum(
        1 for m in models if not label_resolution[m["id"]].get("blocked")
    )
    eligible = total_dets * unblocked_count

    if total_dets == 0:
        return {
            "state": "will-run",
            "summary": (
                "Will run — no detections cached yet "
                "(MegaDetector will run first)"
            ),
            "detail": {
                "total_dets": 0,
                "photos_with_dets": 0,
                "models": [m["name"] for m in models],
                "pending": 0,
                "eligible": 0,
            },
        }

    blocked = []
    pending_per_model = {}
    pending_total = 0
    for m in models:
        info = label_resolution[m["id"]]
        if info.get("blocked"):
            blocked.append(m["name"])
            continue
        fp = info["fingerprint"]
        if params.reclassify:
            pending = total_dets
        else:
            pending = db.count_classify_pending_pairs(
                classifier_model=m["name"],
                labels_fingerprint=fp,
                photo_ids=photo_ids,
            )
        if pending:
            pending_per_model[m["name"]] = pending
            pending_total += pending

    if blocked and not pending_total:
        return {
            "state": "will-run",
            "summary": (
                f"Blocked — {len(blocked)} model{_plural(len(blocked))} "
                f"need labels: {', '.join(blocked)}"
            ),
            "detail": {
                "blocked_models": blocked,
                "pending": 0,
                "eligible": eligible,
            },
        }

    if pending_total == 0:
        return {
            "state": "done-prior",
            "summary": (
                f"Already classified — {total_dets} "
                f"detection{_plural(total_dets)} across "
                f"{len(models)} model{_plural(len(models))}"
            ),
            "detail": {
                "total_dets": total_dets,
                "photos_with_dets": photos_with_dets,
                "models": [m["name"] for m in models],
                "pending": 0,
                "eligible": eligible,
            },
        }

    if params.reclassify:
        summary = (
            f"Re-classify — {pending_total} "
            f"detection-model pair{_plural(pending_total)} "
            f"({total_dets} detection{_plural(total_dets)} × "
            f"{len(models)} model{_plural(len(models))})"
        )
    else:
        breakdown = ", ".join(
            f"{n} for {name}" for name, n in pending_per_model.items()
        )
        summary = (
            f"Will classify {pending_total} new "
            f"pair{_plural(pending_total)} ({breakdown})"
        )
    detail = {
        "pending_pairs": pending_total,
        "per_model": pending_per_model,
        "total_dets": total_dets,
        "photos_with_dets": photos_with_dets,
        "pending": pending_total,
        "eligible": eligible,
    }
    if blocked:
        detail["blocked_models"] = blocked
    return {"state": "will-run", "summary": summary, "detail": detail}
```

### Step 5: Run passing

```bash
python -m pytest vireo/tests/test_pipeline_plan.py -k "exposes_pending_and_eligible" -v
```

Expected: 5 PASS.

### Step 6: Run full plan-tests for regression check

```bash
python -m pytest vireo/tests/test_pipeline_plan.py -v
```

Expected: all PASS — existing tests still green (added keys are additive, existing assertions don't check for absence).

### Step 7: Commit

```bash
git add vireo/pipeline_plan.py vireo/tests/test_pipeline_plan.py
git commit -m "pipeline-plan: uniform detail.pending + detail.eligible for Classify

UI work in PR #2 needs uniform per-stage count fields so the pill
formatter and progress bar don't have to know about pending_pairs vs
pending vs (other stage idiosyncrasies). Extract and EyeKeypoints
already conform; this brings Classify in line. Group is intentionally
skipped (no per-photo unit).

For Classify the work unit is detection-model pairs:
  eligible = total_dets * unblocked_models
  pending  = pending_pairs (or eligible when reclassify=True)

5 new tests cover all six return paths."
```

---

## Task 1.2: Verify focused suite + push branch

**Step 1: Project's full focused suite**

```bash
python -m pytest tests/test_workspaces.py vireo/tests/test_db.py vireo/tests/test_app.py vireo/tests/test_photos_api.py vireo/tests/test_edits_api.py vireo/tests/test_jobs_api.py vireo/tests/test_darktable_api.py vireo/tests/test_config.py vireo/tests/test_pipeline.py vireo/tests/test_pipeline_job.py vireo/tests/test_pipeline_plan.py
```

Triage: the two pre-existing keyword-edit failures from `MEMORY.md` are OK (`test_remove_keyword_from_photo`, `test_undo_keyword_remove_clears_pending_change`). Anything else is on us.

**Step 2: Push and open PR**

```bash
git push -u origin claude/pipeline-pills
gh pr create --base main --title "pipeline-plan: uniform detail.pending + detail.eligible for Classify" --body "$(cat <<'EOF'
## Summary

Phase 2 PR #1 of the pipeline status makeover ([design](docs/plans/2026-05-08-pipeline-status-makeover-phase2-design.md)). Backend-only: every return path of \`_classify_plan\` now exposes uniform \`detail.pending\` + \`detail.eligible\`. \`_extract_plan\` and \`_eye_keypoints_plan\` already conform.

The upcoming PR #2 (UI: pill formatter + progress bar) needs these uniform fields so the JS doesn't have to know about \`pending_pairs\` vs \`pending\` per stage.

For Classify the unit is detection-model pairs:
- \`eligible = total_dets * len(unblocked_models)\`
- \`pending = pending_pairs\` (or \`eligible\` when \`reclassify=True\`)

## Test plan
- [x] 5 new tests in \`test_pipeline_plan.py\` cover all six return paths (skipped, no models, no detections, blocked-only, done-prior, will-run with mix; plus reclassify).
- [x] Existing \`test_pipeline_plan.py\` tests stay green (additive change).
- [x] Focused project suite green minus the 2 known pre-existing keyword-edit failures.

EOF
)"
```

---

## Out of scope

- UI consumption of these fields — that's PR #2.
- Removing the existing `pending_pairs` / `total_dets` / `per_model` / `photos_with_dets` keys — kept for the existing summary-text builders and back-compat.
- Group stage — design intentionally omits since it has no per-photo unit.
