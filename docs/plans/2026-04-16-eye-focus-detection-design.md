# Eye-Focus Detection

## Problem

Vireo's current focus signal (`subject_tenengrad` in `vireo/quality.py`) measures sharpness over the entire SAM2 subject mask. For wildlife photography, this is the wrong granularity: a frame with a soft eye and a sharp body reads as "in focus" to the scorer but "garbage" to the photographer. Conversely, a shallow-DOF portrait with a tack-sharp eye and a soft body is scored as if it were out of focus because most of the masked pixels are soft.

The field-standard wildlife-photography rule is "the eye must be sharp." We want a scoring signal that matches that rule, while degrading gracefully on species and poses where we cannot locate an eye.

## Design Decisions

- **Eye focus replaces body focus when we confidently locate the eye.** Not additive, not a blend. When the keypoint model confidently returns an eye coord inside the subject mask, `eye_focus_score` becomes the focus term in `quality_composite`. When we cannot locate an eye, the focus term is `subject_focus` as it is today. The user's score for any given photo is based on the best signal we have for *that* photo.
- **Same threshold `T` gates two behaviors.** If `eye_conf ≥ T` (and the other gates pass), we *both* use `eye_focus_score` in the composite *and* make the `reject_eye_soft` hard-reject rule eligible. One knob, two effects. Tuning `T` stays coherent.
- **Best eye wins.** Most wildlife portraits have shallow DOF; the near eye is what the photographer focused on and the far eye being soft is an aesthetic, not a flaw. `eye_focus_score = max` over visible eyes that pass the confidence gate.
- **Mask containment is a gate.** A keypoint model run on the MegaDetector crop can latch onto the wrong animal in a multi-subject frame. The detected eye coordinate must fall inside the SAM2 subject mask — otherwise we throw it out. Point-in-polygon test, nearly free.
- **Two models, taxonomy-routed.** SuperAnimal-Quadruped for mammals, SuperAnimal-Bird for birds. Route by `species_top5[0]`'s class lineage (`Mammalia` or `Aves`) via the iNat taxonomy already in the `taxa` table. Everything else (fish, reptiles, insects, unknown) skips keypoint detection and falls back to `subject_focus`.
- **De-risk the pipeline before exporting SuperAnimal.** First integration uses RTMPose-animal (MMPose, first-party ONNX via `mmdeploy`). This validates ONNX loading, heatmap decoding, window sizing, scoring integration, DB schema, and UI — all the downstream work — against a model with clean tooling. Only then do we wrestle with SuperAnimal's export.
- **Opt-in weights download, matching SAM2/DINOv2 pattern.** A new "Eye-focus detection" stage appears on the pipeline page. Users who haven't downloaded weights see a download card (mirroring the SAM2/DINOv2 cards added in #579); users who haven't downloaded get `eye_*` columns that stay null and scoring falls back to `subject_focus` — no behavior change.
- **Null-safe migration.** Adding 4 nullable columns with null defaults means existing photos score identically to today. Scoring changes only take effect on the next pipeline replay.

## Models

**Production (V1):**
- **SuperAnimal-Quadruped** (DeepLabCut 3.x, HuggingFace). ~90 quadruped species zero-shot. Returns ~39 keypoints including `left_eye` and `right_eye`. Used when `species_top5[0]` lineage contains `Mammalia`.
- **SuperAnimal-Bird** (DeepLabCut 3.x, HuggingFace). Bird-specific keypoint set including eye. Used when lineage contains `Aves`.

**Integration-spike (ships first, may stay or may be removed):**
- **RTMPose-animal** (MMPose, trained on AP-10K). Small (~30 MB), transformer-based, first-party ONNX via `mmdeploy`. Returns 17 keypoints including eyes for quadrupeds. Used to prove out ONNX export, decoding, scoring integration, UI, and DB migration before touching SuperAnimal. Decision about whether to keep RTMPose in the shipped app (as a faster alternative or a fallback) deferred until after we have working SuperAnimal integration.

**Export:** add `export_rtmpose_animal()`, `export_superanimal_quadruped()`, and `export_superanimal_bird()` to `scripts/export_onnx.py`, following the existing wrapper + `torch.onnx.export` + `_validate_onnx` pattern. Weights upload to `jss367/vireo-onnx-models`.

**Preprocessing parity is the #1 export risk.** Each model has a specific input size, normalization, and aspect-ratio-preserving resize-with-padding scheme. Mismatch between PyTorch and ONNX at the preprocessing layer manifests as garbage keypoints, not crashes. Validation must compare decoded keypoint coordinates (not raw heatmap tensors) between PyTorch and ONNX Runtime, with tolerance in pixels (±1 px).

## Three-Gate Trust Policy

For a given photo, the eye signal is *trusted* — used in the composite and eligible to trigger `reject_eye_soft` — if and only if **all** of the following hold:

1. `classifier_top1_conf ≥ C` **and** `species_top5[0]` lineage contains `Aves` or `Mammalia`. Default `C = 0.5`. Filters out shaky classifications and non-vertebrate taxa.
2. The appropriate keypoint model's weights are present on disk (user has completed the download) and the model ran without error.
3. At least one eye keypoint has `conf ≥ T`. Default `T = 0.5` (tune from SuperAnimal paper's recommended range; validate on real data).
4. The detected eye coordinate `(x, y)` lies inside the SAM2 subject mask.

If any gate fails, `eye_x`, `eye_y`, `eye_conf`, `eye_focus_score` all stay null. The composite uses `subject_focus` as it does today.

## Scoring Math

For each eye keypoint `(x_k, y_k, conf_k)` where `conf_k ≥ T` and `(x_k, y_k)` is inside the mask:

1. Window side length: `w = round(k * min(bbox_w, bbox_h))`, where `k = 0.08` by default, `bbox` is the MegaDetector bounding box in image-pixel space.
2. Extract the `w × w` window centered on `(x_k, y_k)`, clamped to image bounds.
3. Compute multi-scale Tenengrad on the grayscale window using the existing `_multiscale_tenengrad` helper in `vireo/quality.py`.

Then:

- `eye_focus_raw = max_k eye_tenengrad_k`
- Normalize within encounter using the same percentile normalization that `subject_focus` uses in `vireo/scoring.py` (so `eye_focus_score ∈ [0, 1]` and has the same semantics as `subject_focus`).
- "Best eye" (for DB storage): the eye whose per-keypoint tenengrad was the maximum. Its `(x, y, conf)` is what lands in the `eye_x`, `eye_y`, `eye_conf` columns.

**Composite change in `score_encounter()`:**

```
if eye_focus_score is not null and all gates passed:
    focus_term = eye_focus_score
else:
    focus_term = subject_focus
q = w_focus * focus_term + w_exposure * e + w_composition * c + w_area * a + w_noise * n
```

All other weights and score components are unchanged.

## Reject Rule

New hard-reject rule parallel to `out_of_focus`:

```
if gates passed and eye_focus_score < reject_eye_focus:
    reject_reasons.append(f"eye_soft (E={eye_focus_score:.3f} < {reject_eye_focus})")
```

Default `reject_eye_focus = 0.35` (mirrors the existing `reject_focus` threshold). Only fires when the eye signal is trusted — never rejects a photo on the basis of a low-confidence or outside-mask eye detection.

## Database Schema

Add four nullable columns to `photos`:

| Column            | Type    | Meaning                                                          |
|-------------------|---------|------------------------------------------------------------------|
| `eye_x`           | REAL    | x coord of best eye in image pixels                              |
| `eye_y`           | REAL    | y coord of best eye in image pixels                              |
| `eye_conf`        | REAL    | keypoint confidence of the best eye, ∈ [0, 1]                   |
| `eye_focus_score` | REAL    | normalized focus score around the best eye, ∈ [0, 1]            |

All null when any gate fails. A single "best eye" row per photo rather than storing both eyes separately — the max-over-eyes decision is made at computation time, and we keep only the winner.

Migration: `ALTER TABLE photos ADD COLUMN ...` for each of the four. Safe on existing SQLite because all are nullable with implicit null default.

## Pipeline Staging

New stage in `vireo/pipeline.py` between the existing masking stage and the quality-scoring stage:

```
... → mask_photos → detect_eye_keypoints → score_quality → score_encounter → MMR triage → ...
```

`detect_eye_keypoints` is the new stage. For each photo with a subject mask and a species prediction that routes to a supported keypoint model:

1. Crop the image to the MegaDetector bbox (with a modest margin — match whatever margin masking already uses).
2. Run the routed keypoint model on the crop.
3. Decode heatmaps to `(x, y, conf)` keypoints (argmax + subpixel refinement, standard MMPose / DLC decoding; ~30 lines of numpy).
4. Translate eye coords from crop-space back to image-pixel space.
5. Apply the three-gate policy.
6. If gated through, compute per-eye tenengrad (raw values), pick the winner, persist `(eye_x, eye_y, eye_conf)`.

Normalization into `eye_focus_score ∈ [0, 1]` happens in the next stage (scoring), where per-encounter statistics are available. This matches how `subject_focus` is computed today.

**Replay:** existing pipeline replay machinery. The stage should be re-runnable independently when model weights appear (e.g., user downloads SuperAnimal weights after first pipeline run), without forcing a full re-score.

## UI

One visible change in V1: the review lightbox draws a small crosshair or ring at `(eye_x, eye_y)` when that column is non-null. Visible at any zoom level; aligns with the photographer workflow of pixel-peeping the eye at 1:1.

No score-panel changes in V1. A badge showing "eye-based focus: 0.62 (replaced body focus 0.54)" is the obvious V1.1 addition once the feature has been dogfooded and we know what framing makes sense to users.

Also needed: a new "Eye-focus detection" stage card on `/pipeline`, mirroring the SAM2/DINOv2 cards that show model-download status. Card states: "Model weights not downloaded → [Download SuperAnimal-Quadruped (~200 MB)]", "Running…", "Ready".

## Configuration

New config keys (global defaults, per-workspace overridable via the existing `config_overrides` JSON column on `workspaces`):

| Key                         | Default | Purpose                                                                 |
|-----------------------------|---------|-------------------------------------------------------------------------|
| `eye_detect_enabled`        | `true`  | Master switch for the stage                                             |
| `eye_classifier_conf_gate`  | `0.5`   | `C` — classifier top-1 confidence required to attempt keypoint detection |
| `eye_detection_conf_gate`   | `0.5`   | `T` — keypoint-model eye confidence required to trust the signal         |
| `eye_window_k`              | `0.08`  | Window size as fraction of `min(bbox_w, bbox_h)`                        |
| `reject_eye_focus`          | `0.35`  | `eye_focus_score` below which `reject_eye_soft` fires                   |

No new composite weights — the existing `w_focus` serves both the old and new focus term.

## Failure Modes Handled

| Failure                                           | Result                                                                    |
|---------------------------------------------------|---------------------------------------------------------------------------|
| User hasn't downloaded keypoint weights           | Stage skipped; columns null; scoring uses `subject_focus`                 |
| Classifier low confidence                         | Gate 1 fails; columns null                                                |
| Species is fish / reptile / insect / unknown      | Gate 1 fails; columns null                                                |
| Keypoint model runs but no high-confidence eyes   | Gate 3 fails; columns null                                                |
| Detected eye keypoint falls outside subject mask  | Gate 4 fails; columns null                                                |
| Multi-subject frame, wrong eye picked             | Typically caught by gate 4 (mask containment)                             |
| Photo has no subject mask (masking stage skipped) | Stage skipped for this photo; columns null                                |
| Back-of-head, occluded, profile shots             | Gate 3 usually fails (low eye conf); columns null                         |

In every failure case, composite math falls back to `subject_focus` — there is no "new scoring regression path" for photos that don't get the eye signal.

## Migration

No rescoring required on upgrade. The migration just adds four nullable columns. Existing photos have null `eye_*` values and score identically to the pre-feature behavior.

First pipeline replay after the user downloads keypoint weights computes `eye_*` for eligible photos and applies the new composite and reject rule.

## Implementation Scope

**New files:**
- `vireo/keypoints.py` — weights download, ONNX session cache, `detect_keypoints(image, bbox, species_class) → [(name, x, y, conf), ...]`. Mirrors `vireo/detector.py` shape.
- `scripts/export_onnx.py` additions — three new `export_*` functions (RTMPose-animal, SuperAnimal-Quadruped, SuperAnimal-Bird) following existing patterns.

**Modified:**
- `scripts/export_onnx.py` — extend `ALL_MODELS` and `_EXPORT_FUNCTIONS`.
- `vireo/pipeline.py` — new `detect_eye_keypoints` stage between mask + score.
- `vireo/quality.py` — new `compute_eye_tenengrad(image, eye_xy, bbox, k) → float`.
- `vireo/scoring.py` — composite now uses `eye_focus_score` when gates pass; new `reject_eye_soft` rule.
- `vireo/db.py` — migration adding 4 nullable columns; photo CRUD updated.
- `vireo/taxonomy.py` — helper `classify_to_keypoint_group(top1_taxon_id) → 'Aves' | 'Mammalia' | None`.
- `vireo/app.py` — new `/api/models/keypoints/status` and download endpoints (mirror SAM2/DINOv2).
- `vireo/templates/pipeline.html` — new "Eye-focus detection" card.
- `vireo/templates/_navbar.html` or the specific lightbox container — crosshair overlay at `(eye_x, eye_y)`.
- `vireo/templates/settings.html` — four new tunable thresholds.

**Tests:**
- Unit tests for keypoint loading, decoding, coordinate mapping.
- Scoring tests: gate combinations (each gate failing in isolation, all passing, both-eye case), max-over-eyes, replace-vs-fallback composite behavior, reject-rule firing only when gated through.
- Quality tests: `compute_eye_tenengrad` window sizing, boundary clamping.
- Pipeline tests: stage runs when weights present; skipped gracefully when absent; stage is independently replayable.
- DB tests: migration adds columns; CRUD round-trips all four values including null.
- Taxonomy tests: `classify_to_keypoint_group` correctly maps Aves/Mammalia lineages and returns None for everything else.
- UI smoke: crosshair renders when eye_x/eye_y present; hidden when null.

## Open Questions

- **SuperAnimal ONNX export risk.** Export path has been validated in community forks but not first-party. The RTMPose spike de-risks this for the downstream pipeline; the export itself is still work that could turn up surprises (especially in preprocessing parity). If SuperAnimal export turns out to be a real slog, RTMPose-animal alone is a reasonable V1 for mammals — narrower species coverage but proven tooling.
- **Default thresholds `C` and `T`.** Both defaulted to 0.5 based on common practice; real values come from measuring on a held-out set of Vireo-users' photos. Plan to keep these configurable and iterate after dogfooding.
- **Multi-bird-in-frame photos** (e.g. a flock). SAM2 picks one subject mask. If the detected eye in the crop belongs to a different bird than the mask, gate 4 catches it. If it belongs to the same species but is in the mask (overlapping birds), the signal is still valid for *a* bird, just maybe not *the* bird. Acceptable edge case.
- **Cropping margin.** MegaDetector bboxes are tight; some keypoint models work better with a small margin (5–10%). Decide during RTMPose spike based on measured detection rates.
- **Window size `k`.** 0.08 is a guess informed by eye-to-head ratios in most birds and mammals. Validate on a few hundred real photos; a dynamic `k` based on species class may be needed.
- **Per-workspace "disable eye-focus" affordance.** The config key is per-workspace overridable, but we don't plan a dedicated toggle in V1. Add if users ask.
