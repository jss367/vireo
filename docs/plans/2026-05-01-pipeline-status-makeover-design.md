# Pipeline Status Makeover — Design

**Date:** 2026-05-01
**Branch:** `pipeline-rerun-check`
**Depends on:** PR #736 (SAM mask history) — design assumes that PR has merged.

---

## Problem

The pipeline page tells users a coarse, often misleading story about what will happen if they click Start. Two concrete failures:

1. **Cancel-and-resume is opaque.** When a user cancels mid-stage and returns, the stage shows "Already done" if even one photo got an output before the cancel. There's no way to see "you have unfinished work here".
2. **State is checked against the wrong proxy.** Classify's "Already done" pill is gated on `hasDetections`, not on whether classification actually ran. Settings changes (different model, different SAM2 variant, etc.) don't invalidate prior outputs, so "Already done" can mean "done with stale settings".

The redesign replaces binary "any output exists" checks with per-stage counts and a richer state machine, and surfaces the truth in pills, progress bars, and a restructured plan summary.

---

## Goals

- Each stage card shows accurate counts ("X of Y done") with current settings.
- Three+ resolved states for "before run": **none done**, **some done (resume)**, **all done**, **outdated (recompute)**.
- Plan summary buckets stages by what will happen on Start, with counts.
- Backend exposes per-stage counts via a new `stages` block in `/api/pipeline/page-init`.
- Stale detection lives on a per-stage fingerprint (Classify already has one; add for others).

## Non-goals

- ETAs ("≈ 12 minutes remaining"). YAGNI for v1.
- Per-photo drill-in lists ("which photos are remaining"). Stretch.
- Side-by-side variant comparison views (covered by PR 736 follow-ups).

---

## Section 1 — Stage state model

Every stage resolves to one state at any moment, derived from backend data.

| State | Pill text | Meaning |
|---|---|---|
| `idle-none` | "Will run (1500)" | No prior output, will process from scratch |
| `idle-some` | "Resume (266 left)" | Some photos done with current settings; rest pending |
| `idle-all` | "Already done (1500)" | All eligible photos done with current settings |
| `idle-stale` | "Outdated (1234 to redo)" | Output exists but settings changed; will recompute |
| `disabled` | "Will skip" | User unchecked the enable toggle |
| `blocked` | "Will skip — models not installed" | Prerequisite missing (weights, taxonomy, etc.) |
| `running` | "Running… 234 / 1500" | Live progress |
| `done` | "Done (1500)" | Just finished successfully this run |
| `failed` | "Failed at 234 / 1500" | Just failed |

The pill always carries a meaningful count when one exists.

The 6 user-visible stage cards stay the same set: Scan & Import, Thumbnails & Previews, Classify, Extract Features, Eye Keypoints, Group & Score. The two backend-only stages (Detect, Misses) don't get cards but their counts feed cards that depend on them.

---

## Section 2 — API & data model

### `/api/pipeline/page-init` — new `stages` block

Old bare counts (`has_detections`, `has_masks`, `has_sharpness`) stay during the transition; they're removed in Phase 4.

```json
{
  "total_photos": 1500,
  "stages": {
    "scan":     { "total": 1500, "done": 1500, "stale": 0,   "eligible_total": 1500 },
    "previews": { "total": 1500, "done": 1500, "stale": 0,   "eligible_total": 1500 },
    "detect":   { "total": 1500, "done": 1500, "stale": 0,   "eligible_total": 1500, "fingerprint": "megadetector-v6" },
    "classify": { "total": 1500, "done": 1234, "stale": 100, "eligible_total": 1500, "fingerprint": "modelA|labelsHash" },
    "extract": {
      "configured_variant": "sam2-large",
      "configured": { "done": 856, "stale": 0, "missing": 644, "active": 856, "total_eligible": 1500 },
      "other_variants": [
        { "variant": "sam2-small", "count": 1500, "active_count": 644, "stale_count": 0 },
        { "variant": "unknown",    "count": 234,  "active_count": 0,   "stale_count": 234 }
      ]
    },
    "eye_kp":   { "total": 287, "done": 234, "stale": 0, "eligible_total": 287, "fingerprint": "superanimal-quad-v1" },
    "group":    { "computed": true, "stale": false, "last_at": 1714579200, "encounter_count": 142, "fingerprint": "params_hash_xyz" }
  },
  "blockers": {
    "eye_kp": null,
    "extract": null,
    "classify": null
  }
}
```

### Per-stage source of truth

| Stage | `done` query | `stale` query | Fingerprint stored where |
|---|---|---|---|
| scan | `count(*) WHERE timestamp NOT NULL AND width NOT NULL` | always 0 | n/a |
| previews | filesystem cache lookup, batched | n/a | n/a |
| detect | `count(*) FROM detector_runs WHERE model='megadetector-v6'` | n/a (single detector) | model name (constant) |
| classify | `count(DISTINCT photo_id) FROM classifier_runs WHERE model=? AND fingerprint=?` | same with `<>` current | already in `classifier_runs` ✓ |
| extract | covered by PR 736: `photo_masks` rows for configured variant where prompt matches photo's primary detection | `find_stale_masks()` filtered by variant | per-mask provenance in `photo_masks` ✓ |
| eye_kp | `count(*) FROM photos WHERE eye_tenengrad NOT NULL AND eye_kp_fingerprint=?` | same with `<>` | **new** `photos.eye_kp_fingerprint` |
| group | `workspaces.last_group_fingerprint = current` | fingerprint mismatch | **new** `workspaces.last_grouped_at`, `last_group_fingerprint` |

### Migrations (Phase 1)

- `ALTER TABLE photos ADD COLUMN eye_kp_fingerprint TEXT` — backfill NULL → renders "Outdated" until next run.
- `ALTER TABLE workspaces ADD COLUMN last_grouped_at INTEGER`
- `ALTER TABLE workspaces ADD COLUMN last_group_fingerprint TEXT`

PR 736 handles all extract-stage migration concerns.

### Live progress during run

The existing SSE job stream emits per-stage progress. Add `stage_id`, `done`, `total` to those events. Cards listen for their `stage_id` and update `Running… X / Y` in real time. No new endpoint.

---

## Section 4 — UI structure

### Card anatomy (uniform across all 6)

```
┌─ ☑ Classify ──────────────────────────── [Resume (266 left)] ─┐
│                                                                │
│   ████████████████░░░░░░  1234 of 1500 photos                 │
│                                                                │
│   Last run 2h ago · model: ai/cls-bird-v3 · labels: birds.csv │
│                                                                │
│   ▸ Settings (collapsed by default)                            │
│   ▸ Readiness (collapsed; auto-expands when state=blocked)    │
└────────────────────────────────────────────────────────────────┘
```

Common to every card:
- Header row: enable toggle (where applicable) · stage name · state pill (always shows count)
- Progress bar: green = done w/ current settings · amber = stale · gray = remaining
- Provenance line: "Last run X · model/variant · params". Hidden when stage has never run.
- Collapsible Settings (existing inline controls)
- Collapsible Readiness (existing model download status; auto-expands only when blocked)

### Per-stage tweaks

- **Scan / Previews:** no Settings panel, no provenance. Progress bar only.
- **Extract:** above the progress bar, embed PR 736's per-variant coverage table (Set active buttons). Progress bar reflects the *configured* variant only.
- **Eye Keypoints:** provenance shows model variant.
- **Group:** progress bar replaced by single line "142 encounters · last grouped 2h ago · params unchanged". No "X of Y" — Group is workspace-level.

### Plan summary (above Start button)

Five buckets, each one line. Empty buckets hidden.

```
Will run         Scan & Import (1500), Thumbnails & Previews (12 new)
Will resume      Classify (266 left), Extract Features (644 left, sam2-large)
Outdated         Eye Keypoints (234 to redo)
Already done    Group & Score
Will skip        — (empty, hidden)
```

Footer: "≈ 1156 photos to process across 4 stages". (No ETA for v1.)

---

## Section 5 — Implementation phases

Each phase is one PR. Each ends with a green test suite and a usable UI.

### Phase 1 — Backend counts (no UI changes)

- Add `eye_kp_fingerprint` column to `photos`; backfill NULL.
- Add `last_grouped_at`, `last_group_fingerprint` columns to `workspaces`.
- New helper `db.pipeline_stage_counts(workspace_id)` returning the `stages` block.
- Wire into `/api/pipeline/page-init` alongside existing fields (don't remove old yet).
- Tests: per-stage count math, fingerprint mismatch → stale, empty-workspace edge cases.

### Phase 2 — Card redesign + plan summary

- Refactor `_stageStateFor` to derive from new `stages` block instead of bare booleans.
- New pill states (`idle-some`, `idle-stale`, `running` w/ counts) + count text.
- Progress bar component (done / stale / remaining).
- Provenance line per card.
- Restructured plan summary (5 buckets, count beside each name).
- Hold off on changing Extract — it still uses old `hasMasks` for now.
- Tests: stage-state JS unit tests, manual UI smoke.

### Phase 3 — Extract per-variant integration

- Extract card subsumes PR 736 coverage table (move inside card).
- `configured` block in page-init (filter `mask_variant_coverage` by configured variant).
- Stale count per variant via `find_stale_masks()` grouped.
- Tests: configured-variant counts, stale propagation, switching variants updates pill.

### Phase 4 — Pipeline run wiring + cleanup

- SSE job events emit `stage_id`, `done`, `total` per progress tick.
- Cards listen for their `stage_id`; update pill + bar live.
- After stage completes, settle on `done`/`failed` pill with final counts.
- Remove dead old fields from page-init (`has_detections`, etc.) and old `_stageStateFor` branches.

---

## Open questions / future work

- **ETAs.** Skipped for v1; revisit once we have stage-level throughput data from real runs.
- **Per-photo drill-in.** "Which photos are remaining?" — stretch goal, would live behind a "View remaining" link on each card.
- **Misses stage.** Currently has no card. Could surface as a small footer line in the Group card ("12 missed shots flagged").
- **Detect re-runs.** Today there's only one detector model. If we ever support alternates, add a fingerprint there too.

## Dependencies

- PR #736 (SAM mask history) must merge before Phase 3.
- Phases 1, 2, 4 don't depend on PR #736 and can ship in any order relative to it.
