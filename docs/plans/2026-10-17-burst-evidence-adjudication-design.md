# Burst evidence packs and agent adjudication — Design

**Date:** 2026-10-17
**Branch:** `claude/burst-evidence` (PR 1 of 4)

## Problem

Picking the best frame of a wildlife burst has two halves. One is pixel
measurement: is the eye sharp at 1:1, is the subject clipped, is the exposure
blown. Vireo's cull pipeline already does this half subject-first:
MegaDetector finds the animal, SAM2 masks it, `quality.py` scores Tenengrad
inside the mask (with a background ring so a sharp branch behind a soft bird
reads as a bad ratio, not a high score), and the eye-focus stage replaces body
sharpness with a windowed eye score when it can locate the eye inside the mask.
`scoring.score_encounter` ranks frames by percentile within the burst and
`selection.py` picks diverse keepers.

The other half is judgment: wrong head angle, awkward wing, no catchlight, the
detector latched onto the wrong bird in a two-bird frame. The scorer cannot see
these; a vision model can. But a vision model is bad at the first half: on the
October 11 cull a Codex session judged focus from ~400px downscaled crops and a
fixed hand-drawn box per burst, which is exactly the "measured the branch"
failure the mask-gated scorer exists to prevent. Its 55 "focus miss" rejects
were the least trustworthy decisions of the session.

What that session actually lacked was not a smarter judge but evidence: the
pipeline had analyzed 194 of 489 photos and nothing told anyone. The miss flags
were empty on every photo, which looks identical to "no misses found".

## Design

Vireo does the pixel work and hands an agent an **evidence pack per burst**,
not raw photos. The agent is called only where the scorer is unsure. Its
decision is written back through the normal flag path with a reason and a
source so the UI can show *who* rejected a photo and *why*, and undo it.

Three rules shape every piece:

- **Coverage is always stated.** Every pack and every listing says how many of
  its photos actually have quality data. "No misses flagged" and "misses never
  computed" must never render the same (CORE_PHILOSOPHY.md, "No black boxes").
- **1:1 pixels for focus.** The agent judges sharpness from a native-resolution
  window around the detected eye (or subject centre), rendered by Vireo from the
  working copy. Never from a downscaled frame.
- **The scorer decides the clear cases.** Ambiguity is explicit and rule-based
  so the agent's share of the work is bounded and auditable.

### PR 1 — evidence (this branch)

**`GET /photos/<id>/focus-crop`** (media blueprint). A JPEG window at native
pixel scale, `size` px square (default 512, 128–1024), centred on
`(eye_x, eye_y)` when the eye stage located an eye, else on the primary
detection box centre, else the frame centre. `basis=eye|subject|frame` forces
one centre. `mask=1` darkens pixels outside the active SAM mask (the mask PNG
is authored at pipeline resolution and is scaled up with nearest-neighbour)
so the viewer sees exactly which pixels were scored. Response headers:
`X-Vireo-Focus-Basis`, `X-Vireo-Focus-Center` (`x,y` normalised),
`X-Vireo-Focus-Scale` (`1` = native). Source: working copy first, original
fallback, same as `/photos/<id>/crop`, and the working copy's access time is
stamped because an agent reading crops is a user looking at the photo.
Workspace-gated like every other `/photos/<id>/*` route.

**`GET /api/photos/<id>/burst-evidence`** and **`POST /api/photos/burst-evidence`**
(`{"photo_ids": [...]}`) (photos blueprint). The Best Batch response
(`best_batch.build_best_batch_response`: ranked cards, roles, reasons, scores,
eye coordinates) plus:

- `coverage`: `{photos, with_detection, with_mask, with_quality, with_eye}`
  over the burst.
- `ambiguity`: `{needs_review, reasons: [{code, detail}]}` from
  `burst_evidence.assess_ambiguity`. Codes:
  - `unanalyzed_frames` — some frames have no quality features; the ranking
    is over a subset.
  - `close_call` — the top two non-rejected frames are within `margin`
    (query arg, default 0.10, i.e. 10%) of each other in raw sharpness: eye
    Tenengrad when both were scored on an eye window, else subject
    Tenengrad. Composite quality is not the yardstick because the scorer
    ranks it by percentile inside the burst, which puts frames 2% apart in
    sharpness 0.15 apart in quality.
  - `no_eye_located` — eye detection is enabled for the workspace but the best
    frame's focus is body-based (only when eye detection is on; with it off,
    every burst would be ambiguous and the signal would be noise).
  - `subject_touches_edge` — best frame `crop_complete < 0.9`.
  - `multiple_subjects` — the best frame has two or more animal detections
    above the workspace threshold; the scorer may have scored the wrong one.
  - `all_rejected` — every frame failed a hard-reject rule.
- per card: `crop_complete`, `subject_size`, `detection_count`,
  `reject_reasons` (raw), and URLs: `thumbnail_url`, `crop_url`,
  `focus_crop_url`, `focus_crop_mask_url`, `preview_url`, `mask_url`.
- `scoring`: `{eye_detect_enabled, margin}` so the reader knows the basis.

**`GET /api/folders/<id>/bursts`** (folders blueprint). Walks one folder
(not its subtree; bursts never span folders) and chains frames into bursts
with the rules `best_batch_scope` already uses: same filename prefix, digit
width and extension, sequence gap ≤ 2, capture-time gap ≤ 8 s; frames without a
sequence number chain by capture time alone and require timestamps. Singletons
are not bursts. Returns folder-level `coverage` and, per burst, `seed_photo_id`,
`photo_ids`, `sequence_range`, `count`, `flags` (how many are already
flagged/rejected), `coverage`, `best_photo_id` and `ambiguity`. Each burst is
scored with `run_selected_batch_review` (DB rows only, no image decode).
Folders over `MAX_FOLDER_BURST_PHOTOS` (5000) answer 400 with the count; the
import staging folder is the only one near that today.

The burst-chaining and scope SQL move out of `best_batch.py` into
`PhotoRepository` (`burst_scope_rows`, `best_batch_sequence_rows`,
`best_batch_folder_rows`) behind a new `Database.photos` accessor, lowering the
`best_batch.py` connection cap in `test_sql_boundary.py` to 0. The
`get_best_batch_rows_by_ids` forwarding wrapper is deleted and its caller moved
to the accessor, so the forwarding-wrapper count stays at its limit.

The three reads are aliased under `/api/v1/` for the headless API
(`docs/headless-api.md`), because the agent is a headless client.

### PR 2 — decision provenance

`POST /api/photos/<id>/flag` and `POST /api/photos/flags` accept optional
`reason` (≤ 300 chars) and `source` (≤ 60 chars, e.g. `claude-fable-5-1`).
`PhotoReviewService` folds them into the `edit_history` description, so the
decision is undoable exactly like a hand flag and the Undo panel reads
"Rejected — claude-fable-5-1: eye soft at 1:1, focus on branch". Photo detail
gains `flag_note` (`{source, reason, at}` from the latest live flag edit), and
the Browse detail panel and lightbox show it beside the flag. Both writes are
aliased under `/api/v1/`.

No new table: a decision *is* a flag edit, and edit history is already the
audit trail users can see and reverse.

### PR 3 — coverage where culls start

`/api/browse/summary` adds `analysis: {with_detection, with_quality, with_eye,
with_misses}` for the filtered set, and the Browse summary panel renders
"Quality data for 73 of 377 · misses never computed" under the photo count.
Same counts the folder bursts listing reports, from the same SQL.

### PR 4 — the agent

A skill in `~/git/dotfiles/skills/vireo-adjudicate-bursts/`: discover the
instance from `~/.vireo/runtime.json`, list a folder's bursts, refuse to
adjudicate bursts with `unanalyzed_frames` (tell the user to run Process on
the folder first — the scorer's ranking is the evidence, and it is missing),
fetch the focus crops of the ambiguous bursts and look at them, then write
flags with `reason` and `source`. The session's own vision is the judge; no
API key, no per-image API cost, and the user sees every decision in the Undo
panel.

## Validation plan

Before trusting the adjudicator, run the comparison the October 11 folder makes
possible: finish Process on it, then compare the scorer's keeper per burst with
(a) the Codex session's picks in `.context/october-11-photo-review.json` and
(b) a hand pass by Julius at 1:1 on ten bursts. If the scorer agrees most of
the time, the agent is a small adjudicator; if not, the fix is in the scorer or
the eye stage and an agent on top would be laundering bad evidence.

## Rejected alternatives

**A vision model reviews every frame.** Cost scales with the library (70k
photos), and at any affordable resolution the model cannot judge eye
sharpness; the October 11 session demonstrated both.

**A new `adjudications` table.** A decision with provenance is a flag edit with
a reason; edit history already stores, displays and reverses those. A parallel
record would drift from the flag it describes.

**Scoring in the client / skill.** The skill would re-implement
`score_encounter` and drift from Pipeline Review. The server scores; the skill
reads.
