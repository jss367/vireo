# Pipeline Status Makeover — Phase 2 Design

**Date:** 2026-05-08
**Supersedes:** the original Phase 2-4 sketches in `docs/plans/2026-05-01-pipeline-status-makeover-design.md`. PRs #745 (`/api/pipeline/plan`) and #736 (SAM mask history) shipped most of the original Phase 2-3 vision while we were in flight; this doc resets the remaining UI work.

---

## Why a reset

Surveying `vireo/templates/pipeline.html` post-#745/#736/#739:

- **Already shipped:** plan summary box (`pipelinePlanSummary`), pills driven by `/api/pipeline/plan`, SAM mask coverage table on the Extract card.
- **Still missing:** the pills don't visually distinguish "Outdated" from "Will run", don't say "Resume (N left)" for partial completion, and don't carry counts. There's no progress bar on the cards. The fingerprint staleness signal we wired in #739 lands in `will-run` with no visual differentiation.

Net: the headline UX wins from the original design (cancel-and-resume clarity, settings-change-staleness, count surfaces) are still ahead of us — but the work is much smaller than the original plan described.

## Scope

**In:**
- New pill labels: "Resume (N left)", "Outdated (N to redo)", "Will run (N)", "Already done (N)" — formatted from existing plan endpoint state + detail.
- 2-segment progress bar on Classify / Extract / Eye Keypoints cards.
- Uniform `detail.pending` + `detail.eligible` on every stage planner so the UI doesn't need per-stage count knowledge.

**Out (deferred or skipped):**
- Live SSE counts in pills during a run ("Running… 234 / 1500"). Users still get live progress from the existing `.progress-text` lines.
- Provenance line ("Last run 2h ago · model X").
- Removal of legacy `has_detections` / `has_masks` / `has_sharpness` fields from `/api/pipeline/page-init`. Those still feed card "complete" badges and aren't visibly conflicting.

## Section 1 — Pill text formatter

State space stays at three idle states (`will-run` / `will-skip` / `done-prior`) plus running/done/failed. The UI's pill *label* changes:

| Plan `state` + `detail` flags | Pill label |
|---|---|
| `will-skip` | "Will skip" *(unchanged)* |
| `done-prior`, eligible=N | "Already done (N)" |
| `will-run` AND `detail.fingerprint_outdated` or `fingerprint_invalidated` | "Outdated (N to redo)" |
| `will-run` AND `pending < eligible` | "Resume (N left)" |
| `will-run` AND `pending == eligible` (or no prior data) | "Will run (N)" |
| `running` | "Running…" *(unchanged)* |

N comes from `detail.pending` (`will-run` cases) or `detail.eligible` (`done-prior`).

Group is special-cased — no per-photo count, pill stays text-only ("Already done" / "Will re-group …").

The plan endpoint contract is unchanged — staleness flags are already there from #739. The UI gets a smarter `_setPill` formatter.

## Section 2 — Progress bar

Each card with a per-photo unit (Classify, Extract, Eye Keypoints) gets a slim horizontal bar above the existing `.progress-text` line.

Two segments:
- **Filled**: done = `eligible - pending`, width = `done / eligible * 100%`.
- **Empty**: remaining = `pending`, width = `pending / eligible * 100%`.

Color of the filled segment:
- **Green** when current (no fingerprint flag set).
- **Amber** when `detail.fingerprint_outdated` or `fingerprint_invalidated` — communicates "the done portion is actually stale".

Hidden when `eligible == 0`. Not added to Group (workspace-level, no per-photo unit) or Scan/Previews (no card).

CSS: flexbox with two child divs, `transition: width 200ms`. ~10 lines.

JS: `_renderProgressBar(stage, planEntry)` reads `_pipelinePlan.stages[suffix].detail`, sets widths + class. Called from `refreshPipelineUI()` per stage.

**Why 2 segments not 3 (done/stale/remaining):** breaking out a stale fraction would require per-stage stale counts that only Eye Keypoints exposes cheaply today (via `eye_kp_fingerprint` mismatch). Coloring the entire `done` segment amber when *anything* is outdated communicates the same thing without per-stage stale arithmetic.

## Section 3 — Plan endpoint: uniform `pending` + `eligible`

Each stage planner returns its own count keys today (`pending_pairs` for Classify, `pending`/`eligible` for Eye Keypoints, etc.). The UI's pill formatter and progress bar both want the same two numbers per stage — so each stage's `detail` gains:

- `detail.pending` (int ≥ 0) — items still to process.
- `detail.eligible` (int ≥ 0) — total items in scope.

Existing keys stay for back-compat. The new keys are additive.

Per-stage definitions:

| Stage | `pending` | `eligible` |
|---|---|---|
| Classify | `pending_pairs` (sum across models) | `total_dets * len(models_with_resolved_labels)` |
| Extract | photos missing masks in scope | photos with detections in scope |
| Eye Keypoints | already conforms — keep as-is | already conforms |
| Group | omit — UI hides bar when undefined | omit |

## Section 4 — Implementation phasing

**PR #1 — Backend uniformization**
- Add `detail.pending` + `detail.eligible` to Classify and Extract planners in `pipeline_plan.py`.
- Extend existing per-stage tests in `test_pipeline_plan.py` with assertions on the new fields.
- ~30 lines code + ~20 lines test additions. Single commit.

**PR #2 — UI: pill formatter + progress bar**
- Replace `_PILL_LABELS` lookup with a `_formatPillLabel(stage, planEntry)` helper.
- Add `.stage-progress-bar` CSS and `_renderProgressBar(stage, planEntry)` JS.
- `refreshPipelineUI()` calls both per stage after the existing state-set.
- ~80 lines JS + ~30 lines CSS in `pipeline.html`.
- Manual browser smoke (per project's user-first-testing convention):
  - Empty workspace → bars hidden, pills show "Will run (0)".
  - Mid-run cancel → reopen → "Resume (N left)" with partial filled bar.
  - Settings change after clean run → "Outdated (N to redo)" with amber bar.

## What's not in scope (post-Phase-2 hygiene)

- SSE live counts in pills.
- Provenance line.
- Removing legacy `has_*` fields.
- Adding a "Done (N)" pill state for the just-completed stage right after a run (currently "Done").
- Misses stage as a card.

These are real gaps but not load-bearing for the cancel-resume / settings-changed UX the makeover was started for. Revisit when there's a concrete user-visible reason.
