# Pipeline Pills PR #3 — Per-stage outdated flags

**Date:** 2026-05-13
**Builds on:** Phase 2 PR #1 (#747, uniform `detail.pending`/`detail.eligible`) and PR #2 (#748, pill formatter + amber progress bar). Original design: `docs/plans/2026-05-08-pipeline-status-makeover-phase2-design.md`.

---

## Problem

PR #748 wired the pill formatter and progress bar to render an "Outdated" / amber state when a stage's plan entry sets `detail.fingerprint_outdated` or `fingerprint_invalidated`. Today only `_regroup_plan` emits those flags. Per-photo stages (Classify, Extract, Eye Keypoints) surface settings-changed staleness as a higher pending count, so the UI shows generic "Resume (N left)" with a green bar — accurate but visually undifferentiated from genuine partial completion.

The headline ask: when a user changes a model, label set, SAM2 variant, or detection threshold, the affected stage's pill should say "Outdated" with an amber bar — not "Resume" with a green bar.

## Semantic rule

A per-photo stage emits `detail.fingerprint_outdated = true` when **any** photo (or detection-model pair) was previously processed under settings that no longer match the current configuration. Mixed states (some stale + some never-processed) still flag outdated; the count surfaced in the pill is the **stale count**, not total pending. Summary text disambiguates.

The bar's filled portion drops to "done = eligible - pending" — stale photos count as pending. When a fingerprint bumps, a stage that was 80% complete drops to 0% with amber color. Visually correct: the amber color carries the "you had progress, now it's stale" signal.

## Per-stage staleness sources

| Stage | Source of truth | New helper |
|---|---|---|
| Eye Keypoints | `photos.eye_kp_fingerprint != EYE_KP_FINGERPRINT_VERSION` | `db.count_eye_keypoint_stale(photo_ids=None)` |
| Classify | `classifier_runs` row exists for `(detection, current_model)` with `labels_fingerprint != current` AND no row matches current | `db.count_classify_stale(model, fp, photo_ids=None, min_conf=None)` |
| Extract | `photo_masks` row whose stored `(detector_model, prompt_xywh)` ≠ photo's primary detection (existing `find_stale_masks` logic) | `db.count_extract_stale(sam2_variant, photo_ids=None, detector_confidence=None)` |

Each planner adds:
- `detail.stale` (int) — count of items in scope that will be redone due to staleness.
- `detail.fingerprint_outdated = (stale > 0)`.

**Reclassify edge case:** when `params.reclassify` is set on Classify, EVERY pair will redo regardless of cached state — but reclassify is a user-explicit override, not a settings-change signal. Suppress the `fingerprint_outdated` flag when reclassify is on so the pill says "Re-classify (N pairs)" instead of "Outdated (N to redo)".

## Implementation phasing

Single PR, 5 sequential tasks. Each per-stage task is TDD: failing helper test → implement helper → failing planner test → wire helper into planner → green → commit.

### Task 1 — Eye Keypoints (simplest, sets the pattern)

- New `db.count_eye_keypoint_stale(photo_ids=None)` mirroring `count_eye_keypoint_eligible`'s join shape (workspace + mask + detection + prediction) with `eye_tenengrad NOT NULL AND (eye_kp_fingerprint IS NULL OR eye_kp_fingerprint != ?)`.
- Update `_eye_keypoints_plan` in `pipeline_plan.py` to call the helper, set `detail.stale`, set `fingerprint_outdated`.
- 3 tests in `test_pipeline_plan.py`: stale > 0 sets flag, stale = 0 doesn't, fingerprint mismatch counted correctly.

### Task 2 — Extract

- New `db.count_extract_stale(sam2_variant, photo_ids=None, detector_confidence=None)` reusing `find_stale_masks` query logic, filtered by variant and photo_ids.
- Update `_extract_plan` — read `sam2_variant` from `pipeline_cfg` (already available via `effective_cfg`), pass workspace `min_conf`.
- 3 tests: fresh masks → stale = 0, prompt mismatch → counted, scope filter respected.

### Task 3 — Classify

- New `db.count_classify_stale(classifier_model, labels_fingerprint, photo_ids=None, min_conf=None)` — count distinct detections in scope where there's a stale `classifier_runs` row for the model AND no current row.
- Update `_classify_plan` to sum across unblocked models, set `detail.stale` and `fingerprint_outdated`. Suppress flag when `params.reclassify`.
- 3 tests: stale fingerprint detected, current row wins, reclassify suppresses flag.

### Task 4 — Browser visual smoke (Playwright)

Drive an isolated dev server through three scenarios on a temp DB:

1. **Eye Keypoints outdated** — seed eligible photos with old `eye_kp_fingerprint`, bump `EYE_KP_FINGERPRINT_VERSION`, reload, capture pill = "Outdated (N to redo)" + amber bar.
2. **Extract outdated** — seed photo_masks rows, mutate the detection bbox to invalidate prompt match, reload, capture amber state.
3. **Classify outdated** — record `classifier_runs` under `fp1`, change active labels to produce different fingerprint, reload, capture amber state.

Screenshots to `/tmp/pr3-screenshots/`, inline in PR description.

### Task 5 — Push + open PR

Focused project test suite green, push branch, open PR with screenshots.

## Total scope

- 3 new DB helpers (~80 lines)
- 3 planner updates (~30 lines)
- ~9 tests (~120 lines)
- 1 docs commit + 4 implementation commits + 1 browser-smoke commit if needed

## Out of scope

- Live SSE counts in pills during runs.
- Provenance line per card.
- Removing legacy `has_*` consumers from `/api/pipeline/page-init`.

These remain as future polish; the headline staleness-visibility UX is what this PR delivers.
