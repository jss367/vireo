# Review Burst Group — decouple species confirmation from pick/reject apply

**Date:** 2026-05-29
**Status:** Design approved, ready for implementation plan

## Problem

Inside the **Review Burst Group** modal (`vireo/templates/pipeline_review.html`),
there is exactly one commit path: the **"Apply flags/species & Close"** button
(`grmApply()`). It fuses three independent things into a single atomic action:

1. Pick/reject/candidate **flag** decisions.
2. **Species** — tags picks with the species keyword *and* marks the burst confirmed.
3. **Closing** the burst.

So the user cannot independently confirm "yes, this is the species" versus
"yes, I accept the picks and rejects." The two are forced together.

There is also a **backend divergence**: the grid's species ✓ button goes through
`/api/encounters/species` (persists `species_confirmed` server-side, tags *all*
frames, handles species replacement, auto-detaches a burst whose species differs
from its encounter). The modal's `grmApply()` instead folds species into
`/api/pipeline/group/apply`, which tags only the **picks** and marks confirmation
**only in the local pipeline cache** — never server-side. "Confirmed in the modal"
and "confirmed in the grid" are not the same persisted state.

## Decisions

### B — Two checkboxes + one "Apply and close" button

The single fused button is replaced by:

- Checkbox **"Confirm species"** — when committed, tags *every* frame in the burst
  with the species and marks the burst confirmed.
- Checkbox **"Apply picks/rejects"** — when committed, writes the
  pick/reject/candidate flag decisions.
- A single **"Apply and close"** button that commits whichever boxes are checked,
  then closes. If neither is checked it is effectively "Close" (no DB writes).

**Smart defaults (option C):** a box is checked only when there is a *real pending
change*, recomputed live on every zone move and species-field edit:

- "Apply picks/rejects": checked when `grmComputeDiff()` shows
  `flagNew + rejectNew + clearNew > 0`.
- "Confirm species": checked when the species field is non-empty **and** differs
  from the burst's already-confirmed species (`grmState.initialSpecies`).
  Re-confirming an unchanged species is a no-op → stays unchecked.

**Inline warning (option B):** if a checkbox is *unchecked* but its diff is
non-empty (the user overrode a smart-default-checked box), show a small amber
hint beside it — e.g. "3 flag changes won't be saved" / "species won't be
confirmed". No dialog. Closing therefore never silently drops work.

**Button label** keeps today's transparency, scoped to checked boxes only:
"Flag 3 · Reject 2 & Close", "Confirm species & Close", or "No changes & Close".

### C — Backend unification

- **`/api/pipeline/group/apply` becomes flags-only.** The modal stops sending
  `species`. The keyword-tagging block is removed from the endpoint (param left
  accepted-but-ignored or removed, decided by test fallout; the modal is its only
  caller).
- **Species confirmation routes through `/api/encounters/species`**, the same
  endpoint the grid uses, called burst-scoped:
  `{ species, photo_ids: <current burst members>, burst_index: <current idx> }`.
  This gives, identically to the grid: tag all burst frames, persist
  `species_confirmed` / `species_override.confirmed` server-side, species
  replacement (untag old + queue sidecar remove), auto-detach on species mismatch,
  and an authoritative `encounters` + `summary` response that the modal adopts
  into `pipelineResults` (mirroring `confirmSpecies()`).

Net effect: modal and grid confirmation become one persisted state, one code path.

## Data flow / ordering in `grmApply()`

Two restructuring operations can fire in one apply — the modal's existing
"removed photos → detach into single-frame bursts" logic, and species confirm's
auto-detach. `/api/encounters/species` reloads the cache from disk and validates
`photo_ids ⊆ bursts[burst_index]`, so order matters.

1. Read checkbox state → `applyFlags`, `confirmSpecies`.
2. If `applyFlags`: `POST /api/pipeline/group/apply` (flags only) → update local
   `pipelineResults.photos` labels + flags. Abort on failure (return early, do not
   close) so DB and cache cannot drift.
3. Local structural mutations as today (browse-selection handling, removed→detach
   splicing). Recompute the current burst index + remaining member ids.
4. `save-cache(pipelineResults)` → persists flags-as-labels + post-removal
   structure to disk so the next step reads a consistent cache.
5. If `confirmSpecies` (and burst still has ≥1 frame): `POST /api/encounters/species`
   with the **remaining** burst member ids (removed photos excluded, so they are
   not tagged) + current `burst_index`. Adopt returned `encounters` + `summary`
   into `pipelineResults` (the endpoint already re-saved the cache).
6. Close, clear the species field, `renderResults()`, `updateSummaryBar()`.

Rationale: flags are per-photo and structure-independent → first. The modal's own
structural edits are persisted before the species call so the endpoint validates
against the structure the user sees. Species confirm runs last; its response is
authoritative, exactly how the grid treats it.

## Edge cases

- **Empty burst after removal:** skip the species call (nothing to confirm); the
  existing "all removed → fall back to normal review" path is preserved.
- **Browse-selection source:** temporary in-memory bursts may not map to a
  persisted encounter `/api/encounters/species` can find. **Verify live** before
  committing; if unmapped, either keyword-tag-only fallback or gate species-confirm
  off for that source. Do not assume.
- **Species replacement in-modal:** changing an already-confirmed burst's species
  now untags the old one via the unified endpoint (previously impossible cleanly).
- **Unchecked-but-dirty close:** amber hint shows; changes silently discarded on
  close (no dialog).
- **Seed guard:** the existing `grmState.seeded` check stays — Apply is a no-op
  until group state has loaded.

## Testing

- **E2E (Playwright, user-first):** smart-default checkbox states on open; unchecking
  a dirty box shows the amber hint; species-only apply leaves flags untouched + tags
  all frames + marks confirmed; flags-only apply leaves species unconfirmed; both
  checked lands both; species replacement untags the old keyword.
- **Backend:** `group/apply` no longer tags species; the modal species path produces
  the same DB state as a grid `/api/encounters/species` call; auto-detach still fires.
- **Regression:** the CLAUDE.md test bundle.
