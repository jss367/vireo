# Mask Verifier Experiments

**Date:** 2026-05-11
**Status:** Design — not yet implemented
**Scope:** Out-of-tree experiment harness. No changes to Vireo proper until a winning strategy is identified.

## Problem

Vireo's highlights for workspace `May2026` (5,238 photos) include obvious garbage in the top picks. Investigation shows the failure is **bad SAM2 masks**, not a bad scorer.

### Concrete example: DSC_6126 (photo id 43158)

The frame contains two tiny birds at the top of a leafy bush, in shade. The SAM2 mask covers ~56% of the frame, drawn on the bush and ground — not on the birds. Yet `quality_score = 0.834` and it shipped as the #15 highlight.

How it survived every existing gate:

| Signal | Value | Why it didn't reject |
|---|---|---|
| `mask_path` | present | "no_subject_mask" rule only fires when mask is missing entirely |
| `subject_tenengrad` | 279 | Measured over the masked region (bush leaves, mostly OOF) |
| `bg_tenengrad` | 32,905 | Measured over the ring outside the mask — actual sharp branches |
| `subject_size` | 0.564 | sqrt-compressed to area_score = 1.0 — huge area = big bonus |
| `focus_score` | ~0.35 | Encounter percentile rank ≈ 0.5 because siblings are equally bad |
| `out_of_focus` reject | did not fire | percentile rank held f above the 0.35 absolute threshold |

The subject is 100× *less* sharp than its surrounding ring. That alone is diagnostic of a wrong mask, but no current rule looks at the ratio. Encounter-percentile normalization actively hides the absolute badness when all siblings share the same wrong mask.

### Failure modes to address

1. **False positives** — masks drawn on non-subjects (bushes, branches, ground) score high and ship as highlights.
2. **False negatives** — real keepers rejected because the detector missed them (no mask produced).
3. **Burst pollution** — top-N filled by sibling frames from one burst (top 12 in May2026 contains 8 frames from one ~12-frame burst).

This design focuses on **(1)** and **(2)**. Burst pollution is a separate fix (MMR by burst, not just by species) and gets its own design later.

## Approach: experiment first, port the winner later

Build a standalone evaluation harness outside Vireo. Iterate on verifier strategies, measure precision/recall against hand-labeled ground truth, and only port the winning strategy (or combination) into Vireo's pipeline once it clearly beats the current baseline.

Reasons:

- Rerunning Vireo's full pipeline to test a verifier variation takes hours; the experiment loop should be seconds.
- The verifier is a binary classifier — it needs a labeled test set. Better to build that test set as a side product of the experiment than to wedge labeling into Vireo.
- Multiple strategies need to be compared on the same photos. Easier in a focused tool than in production.

## Test set

Stratified sample of ~1000 photos from workspace `May2026`. Strata overlap; dedupe and top up the random bucket to land near 1000.

| Stratum | Target | Tests |
|---|---|---|
| Top decile by `quality_score` | 200 | False positives |
| Bottom-decile rejects (with masks) | 100 | Verifier shouldn't accept these either |
| `no_subject_mask` rejects | 150 | Detection recall — missed keepers |
| Random middle 80% | 300 | Coverage |
| Extreme sub/bg ratio (sub < 0.5× bg) | 100 | The DSC_6126 failure mode |
| Tiny subject (`subject_size < 0.02`) | 100 | Detector edge cases |
| Huge subject (`subject_size > 0.40`) | 50 | "Mask ate the whole bush" |

Sampling is seeded so reruns are deterministic. The photo IDs and stratum tags are written to `out/test_set.json`.

## Labeling schema

Four buttons per photo:

- **GOOD** — real subject present, mask captures it well.
- **BAD MASK** — mask exists but is wrong (drawn on grass, branches, missing the bird, etc.).
- **KEEPER MISSED** — real subject present, but no mask was produced or it was rejected. Diagnoses detection failures separately from mask-verification failures.
- **TRUE NEGATIVE** — no worthwhile subject in frame, correctly excluded.

Plus a free-text notes field per photo.

### Metrics derived from labels

For each verifier strategy:

- **Mask precision** — of masks the strategy accepts, fraction labeled GOOD.
- **Mask recall** — of GOOD-labeled masks, fraction the strategy accepts.
- **True-negative rate** — of TRUE NEGATIVE photos, fraction the strategy correctly rejects.

And separately, a system-level metric independent of any verifier:

- **Detection recall** — `GOOD / (GOOD + KEEPER MISSED)` — diagnoses how often we never had a mask to verify in the first place. If this number is bad, the answer is "improve detection," not "improve verification."

## Verifier strategies

Each strategy is a function `(photo_dict, *, ctx) -> VerifierResult(accept: bool, score: float, why: str)`. The `ctx` carries pre-loaded resources (centroids, encoders) so per-photo calls stay cheap.

Ordered cheapest → most expensive:

**S1 — Sharpness ratio.** Reject when `subject_tenengrad < k * bg_tenengrad` for tunable `k ∈ {0.5, 0.7, 1.0}`. All values are already in the DB. The DSC_6126 case has ratio ≈ 0.008, would be rejected at any k > 0.01.

**S2 — Mask geometry.** Reject masks that (a) touch ≥ 2 frame edges, (b) cover > 40% of the frame, (c) have perimeter/area indicating a scattered or branch-like shape. Geometric only, no model.

**S3 — Global-vs-subject DINO divergence.** If `cosine(dino_subject_embedding, dino_global_embedding) > 0.95`, the "subject" is essentially the whole frame — mask ate the background. Both embeddings already stored.

**S4 — DINO crop vs bird centroid.** Build a "good bird crop" centroid from photos with high-confidence species predictions (bootstrap from the `predictions` table). Cosine-sim each candidate against the centroid; threshold tuned on labels.

**S5 — BioCLIP text-image.** Use `vireo.text_encoder` (already available, ONNX). Score the masked crop against prompts: *"a sharp photo of a wild bird"* vs *"blurry foliage and branches"*. Accept if first wins by a margin.

**S6 — Species classifier on the masked crop.** Re-run `timm_classifier` on the cropped/masked region. Reject if top-1 confidence < threshold or top class isn't a bird taxon. Most semantically meaningful, most expensive.

**Deliberately skipped (for now):** detection-confidence threshold. `detection_conf` is NULL on every photo in the test workspace, so we have nothing to gate on without re-running the detector. Recorded as follow-up.

### Combinations

Once individual precision/recall numbers exist, the harness automatically evaluates AND, OR, and majority-vote combinations of the top performers. The winner may be e.g. "S1 OR S3 (high recall) AND S6 (high precision filter)".

## Harness architecture

Lives outside the Vireo source tree:

```
docs/plans/2026-05-11-mask-verifier-experiments/   # gitignored — research scripts
  build_test_set.py
  strategies.py
  run_strategies.py
  label_server.py
  templates/index.html
  static/app.js
~/.vireo/experiments/mask-verify/                  # outputs (gitignored, off-repo)
  test_set.json
  results.json
  labels.json
  metrics.json
```

### Data flow

1. `build_test_set.py` reads `~/.vireo/vireo.db`, applies the strata, writes `test_set.json`.
2. `run_strategies.py` loads each test photo's row + mask + embeddings, runs every strategy, writes `results.json`. No DB writes.
3. `label_server.py` (Flask, `localhost:5050`) renders one row per photo: thumbnail with mask overlay, stored stats, strategy verdicts in a horizontal strip, four label buttons, notes field. Each label click POSTs to `/label` which appends to `labels.json` atomically. The page recomputes precision/recall from `labels.json` ∩ `results.json` after every click and updates a sticky leaderboard.
4. Adding a new strategy = add a function to `strategies.py`, rerun `run_strategies.py`. Existing labels persist — no relabeling. This is the key property making the harness worth its weight.

### HTML row layout

- Left: thumbnail (click to cycle: original / mask-tinted overlay / mask-isolated crop).
- Middle: stored stats (subject_tenengrad, bg_tenengrad, subject_size, quality_score, encounter_id, predicted species + confidence if any).
- Right: strategy verdicts as cells — `S1: ✅ 0.82`, `S3: ❌ 0.41`, hover for `why`, color-coded.
- Bottom: GOOD / BAD MASK / KEEPER MISSED / TRUE NEGATIVE buttons + notes textarea.

Sticky top bar: progress (`347/1000 labeled`), strategy leaderboard, and filters (unlabeled-only, strategy-disagreement-only, by stratum).

## Success criteria

A verifier strategy is a candidate for porting into Vireo if it meets all of:

- **Mask precision ≥ 0.95** — accepting a bad mask is the failure we're fixing; can't make it worse.
- **Mask recall ≥ 0.85** — must not throw out too many good masks.
- **True-negative rate ≥ 0.90** — doesn't accept obviously-empty frames.
- **Disagrees with the current baseline on the right photos** — when graded against labels, it specifically rejects the false positives the current pipeline accepts (e.g. the DSC_6126 cluster).

If no individual strategy hits all four, the harness's combination evaluator picks the best AND/OR/majority assembly. If no combination hits all four, the design needs revisiting — likely meaning detection itself is the limiting factor (high `KEEPER MISSED` count), not verification.

## What this design does *not* cover

- Burst-aware MMR (different problem, separate design).
- Detection-recall improvements (rerunning MegaDetector at lower thresholds, alternative detectors).
- Persisting `detection_conf` on photos (a small Vireo change, queue as follow-up).
- Absolute focus floor in scoring (a small Vireo change, may be the immediate "patch" while the verifier is being built).
- The eventual port of the winning strategy into Vireo's pipeline (a separate design once we know what won).
