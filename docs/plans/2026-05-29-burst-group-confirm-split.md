# Review Burst Group — Confirm/Apply Split Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Let the user independently commit "confirm this species" and "accept these picks/rejects" inside the Review Burst Group modal, via two smart-defaulted checkboxes and one "Apply and close" button, and unify species confirmation onto the same backend path the grid uses.

**Architecture:** Frontend change concentrated in `vireo/templates/pipeline_review.html` (modal footer markup + the `grmApply()` flow + checkbox state derived from the existing `grmComputeDiff()`). Backend change: `/api/pipeline/group/apply` becomes flags-only; species confirmation routes through the existing `/api/encounters/species` (pipeline bursts) or `/api/batch/keyword` (browse-selection ad-hoc sets).

**Tech Stack:** Flask, Jinja2, vanilla JS (no framework), SQLite, pytest, Playwright (E2E).

**Design doc:** `docs/plans/2026-05-29-burst-group-confirm-split-design.md`

**Decisions baked in:**
- Smart-default checkboxes (option C); inline amber warning when an unchecked box is dirty (option B).
- "Confirm species" tags **all** burst frames (matches the grid), not just picks.
- "Apply picks/rejects" owns flag changes **and** removed-photo detach (`detachNew`).
- Backend: `group/apply` stops handling species; species goes through
  `/api/encounters/species` (pipeline) / `/api/batch/keyword` (browse-selection).

---

## Task 1: Make `/api/pipeline/group/apply` flags-only

Remove species keyword tagging from the endpoint. The modal will stop sending
`species`; species confirmation moves entirely to other endpoints (Task 4).

**Files:**
- Modify: `vireo/app.py:12707-12820` (`api_pipeline_group_apply`)
- Test: `vireo/tests/test_pipeline_group_apply.py`

**Step 1: Update the failing test**

Find the existing test(s) in `vireo/tests/test_pipeline_group_apply.py` that pass
`species` and assert picks get the species keyword. Change the assertion to the
new contract: posting `species` does **not** tag any photo. Add a focused test:

```python
def test_group_apply_ignores_species_and_tags_nothing(client, ...):
    # picks=[p1], rejects=[p2], species provided
    resp = client.post("/api/pipeline/group/apply", json={
        "picks": [p1], "rejects": [p2], "candidates": [],
        "species": "Blue Jay",
    })
    assert resp.status_code == 200
    body = resp.get_json()
    # flags still applied
    assert body["photos"][str(p1)]["flag"] == "flagged"
    assert body["photos"][str(p2)]["flag"] == "rejected"
    # but NO species keyword was added to the pick
    assert body["photos"][str(p1)]["has_species_keyword"] is False
    # and the DB has no species keyword on p1
    kws = get_db().get_photo_keywords(p1)
    assert not any(k["name"] == "Blue Jay" for k in kws)
```

(Use the file's existing fixtures/helpers for `client`, photo creation, and
`get_db`. Mirror the setup of the current species test in that file.)

**Step 2: Run test to verify it fails**

Run: `python -m pytest vireo/tests/test_pipeline_group_apply.py -v`
Expected: FAIL — current endpoint tags the pick, so `has_species_keyword` is True.

**Step 3: Remove species handling from the endpoint**

In `vireo/app.py` `api_pipeline_group_apply` (lines ~12707-12820):

- Delete the `species` parsing line:
  `species = (body.get("species") or "").strip()`
- Delete the species-precompute block (lines ~12743-12750):
  ```python
  species_kid = None
  photos_with_species = set()
  if species:
      species_kid = db.add_keyword(species, is_species=True)
      for pid in picks:
          ...
  ```
- Delete the keyword-tagging block inside the `try:` (lines ~12773-12785):
  ```python
  kw_added_pids = []
  if species and species_kid is not None:
      for pid in picks:
          ...
  ```
- Delete the `kw_added_pids` edit-history block (lines ~12802-12807).
- In the response (lines ~12810-12819), replace the `has_species_keyword`
  computation that referenced `species_kid` with a constant `False`:
  ```python
  result_photos[pid] = {
      "flag": row["flag"] or "none",
      "has_species_keyword": False,
  }
  ```
  (Kept in the payload so the client cache-sync code in `grmApply` need not
  change shape; it is now always False because this endpoint no longer tags.)
- Update the docstring's first paragraph to say it applies flag decisions only.

**Step 4: Run test to verify it passes**

Run: `python -m pytest vireo/tests/test_pipeline_group_apply.py -v`
Expected: PASS.

**Step 5: Run the broader app/pipeline suites for fallout**

Run: `python -m pytest vireo/tests/test_pipeline_group_apply.py vireo/tests/test_app.py -q`
Expected: PASS (fix any test that asserted the old species-tagging behavior to
match the new flags-only contract — do not weaken unrelated assertions).

**Step 6: Commit**

```bash
git add vireo/app.py vireo/tests/test_pipeline_group_apply.py
git commit -m "pipeline: make /group/apply flags-only; species moves to dedicated path"
```

---

## Task 2: Footer markup — two checkboxes + "Apply and close"

Replace the single fused button with two checkboxes and one apply button, plus
hidden amber-hint spans. Pure markup + CSS; behavior is wired in Task 3.

**Files:**
- Modify: `vireo/templates/pipeline_review.html:1458-1479` (the `.grm-footer`)
- Modify: `vireo/templates/pipeline_review.html` CSS block (near the other
  `.grm-*` styles, e.g. around lines 900-990) — add `.grm-commit-toggle` and
  `.grm-dirty-hint` styles.

**Step 1: Replace the apply button markup**

In `.grm-footer`, replace the single button (line 1478) with:

```html
<div class="grm-commit-group" style="margin-left:auto;display:flex;align-items:center;gap:14px;">
  <label class="grm-commit-toggle">
    <input type="checkbox" id="grmConfirmSpeciesChk" onchange="grmOnToggleChange()">
    <span>Confirm species</span>
    <span class="grm-dirty-hint" id="grmSpeciesDirtyHint" style="display:none;"></span>
  </label>
  <label class="grm-commit-toggle">
    <input type="checkbox" id="grmApplyFlagsChk" onchange="grmOnToggleChange()">
    <span>Apply picks/rejects</span>
    <span class="grm-dirty-hint" id="grmFlagsDirtyHint" style="display:none;"></span>
  </label>
  <button id="grmApplyBtn" onclick="grmApply()" class="btn-batch-accept"
    style="padding:8px 20px;border:none;border-radius:4px;font-size:13px;font-weight:600;cursor:pointer;background:var(--accent);color:var(--accent-text);"
    title="Commit whichever boxes are checked, then close this burst.">Apply and close</button>
</div>
```

Keep the existing `Species:` input (line 1459-1460), Remove button, thumb/res/zoom
controls, and `grmHint` exactly as they are — only the trailing button is replaced.

**Step 2: Add CSS**

Near the other `.grm-*` rules:

```css
.grm-commit-toggle { display:flex; align-items:center; gap:6px; font-size:12px;
  color: var(--text-muted); cursor:pointer; user-select:none; }
.grm-commit-toggle input { cursor:pointer; }
.grm-dirty-hint { font-size:11px; color: var(--warning, #e0a800); }
```

(If `--warning` is not a defined theme var, use a literal amber `#e0a800`.)

**Step 3: Verify it renders**

Run the app, open a burst group, confirm two checkboxes + "Apply and close" show.
Run: `python vireo/app.py --db ~/.vireo/vireo.db --port 8080` and load the
pipeline review page. (No automated test in this task — covered by Task 5 E2E.)

**Step 4: Commit**

```bash
git add vireo/templates/pipeline_review.html
git commit -m "pipeline review: split burst-group apply into species/flags checkboxes (markup)"
```

---

## Task 3: Checkbox state — smart defaults + amber hints

Wire the checkboxes' default/derived state from the existing `grmComputeDiff()`,
and show amber hints when an unchecked box is dirty. Track whether the user has
manually overridden each box so smart-default recomputation doesn't fight them.

**Files:**
- Modify: `vireo/templates/pipeline_review.html` — `grmState` init in
  `openGroupReview` (lines ~3578-3604); `grmUpdateApplyLabel` (lines ~3879-3897);
  add new functions near it.

**Step 1: Add override-tracking to `grmState`**

In `openGroupReview`'s `grmState = {...}` (line 3578), add:

```js
    // null = follow smart default; true/false = user explicitly set it.
    confirmSpeciesOverride: null,
    applyFlagsOverride: null,
```

**Step 2: Add the toggle + sync functions**

Add near `grmUpdateApplyLabel`:

```js
// User manually toggled a checkbox: record the override, then re-sync labels.
function grmOnToggleChange() {
  var spChk = document.getElementById('grmConfirmSpeciesChk');
  var flChk = document.getElementById('grmApplyFlagsChk');
  if (spChk) grmState.confirmSpeciesOverride = spChk.checked;
  if (flChk) grmState.applyFlagsOverride = flChk.checked;
  grmUpdateApplyLabel();
}

// Smart default: is there a real pending change for each side?
function grmSpeciesDirty(diff) { return !!diff.speciesChanged; }
function grmFlagsDirty(diff) {
  return (diff.flagNew + diff.rejectNew + diff.clearNew + diff.detachNew) > 0;
}

// Resolve the effective checked state: user override wins, else smart default.
function grmResolveChecks(diff) {
  var sp = grmState.confirmSpeciesOverride;
  var fl = grmState.applyFlagsOverride;
  return {
    species: sp === null ? grmSpeciesDirty(diff) : sp,
    flags: fl === null ? grmFlagsDirty(diff) : fl,
  };
}
```

**Step 3: Drive checkbox + hint state from `grmUpdateApplyLabel`**

In `grmUpdateApplyLabel`, after `var diff = grmComputeDiff();`, set the checkbox
checked state and amber hints, and scope the button label to checked boxes:

```js
  var checks = grmResolveChecks(diff);
  var spChk = document.getElementById('grmConfirmSpeciesChk');
  var flChk = document.getElementById('grmApplyFlagsChk');
  if (spChk) spChk.checked = checks.species;
  if (flChk) flChk.checked = checks.flags;

  // Amber hint only when a box is unchecked but its side is dirty.
  var spHint = document.getElementById('grmSpeciesDirtyHint');
  if (spHint) {
    var show = !checks.species && grmSpeciesDirty(diff);
    spHint.style.display = show ? '' : 'none';
    spHint.textContent = show ? "species won't be confirmed" : '';
  }
  var flHint = document.getElementById('grmFlagsDirtyHint');
  if (flHint) {
    var n = diff.flagNew + diff.rejectNew + diff.clearNew + diff.detachNew;
    var showF = !checks.flags && n > 0;
    flHint.style.display = showF ? '' : 'none';
    flHint.textContent = showF ? (n + ' cull change' + (n === 1 ? '' : 's') + " won't be saved") : '';
  }
```

Then change the existing label-building so `parts` only includes a side when its
box is checked: guard the species parts (`Set species`, `Tag N`) behind
`checks.species`, and the flag parts (`Flag/Reject/Clear/Detach`) behind
`checks.flags`. Keep the `'No DB changes & Close'` fallback, but change the
suffix to match the new button: when `parts.length` is 0 set
`btn.textContent = 'Apply and close'`, else `parts.join(' · ') + ' & Close'`.

**Step 4: Verify in the app**

Open a burst: with pending flag moves, "Apply picks/rejects" is pre-checked; type
a new species → "Confirm species" pre-checks; uncheck one with changes → amber
hint appears; button label updates to only the checked side.

**Step 5: Commit**

```bash
git add vireo/templates/pipeline_review.html
git commit -m "pipeline review: smart-default checkboxes + amber dirty hints"
```

---

## Task 4: Rewrite `grmApply()` to commit only checked sides

Split the fused apply into flags-only `/group/apply` + dedicated species path,
in the order the design fixed.

**Files:**
- Modify: `vireo/templates/pipeline_review.html:4747-4893` (`grmApply`)
- Add: a helper `grmConfirmSpeciesCall(...)` near `grmApply`.

**Step 1: Read effective checkbox state at the top of `grmApply`**

After the `grmState.seeded` guard (line 4754), add:

```js
  var diff = grmComputeDiff();
  var checks = grmResolveChecks(diff);
  var species = document.getElementById('grmSpecies').value.trim();
```

(Replace the existing `var species = ...` line 4755.)

**Step 2: Gate the flags `/group/apply` call behind `checks.flags`**

Wrap the existing `/api/pipeline/group/apply` fetch + local label/flag sync
(lines ~4757-4816) in `if (checks.flags) { ... }`. Stop sending `species` in the
body (remove the `species: species` line ~4777). On fetch failure, keep the
existing early `return` (abort before close).

When `checks.flags` is false: skip the call, and also skip the removed-photo
detach block (Step 4) — removals are part of "cull changes".

**Step 3: Add the species-confirm helper**

```js
// Confirm species for the burst. Pipeline bursts go through the same endpoint
// the grid uses (persists confirmation + auto-detach). Browse-selection ad-hoc
// sets have no encounter, so just tag the keyword on all members.
async function grmConfirmSpeciesCall(species, memberIds) {
  if (!species || memberIds.length === 0) return;
  if (grmState.source === 'browse-selection') {
    await safeFetch('/api/batch/keyword', {
      method: 'POST', headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ photo_ids: memberIds, name: species, type: 'taxonomy' }),
    });
    return;
  }
  var resp = await safeFetch('/api/encounters/species', {
    method: 'POST', headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ species: species, photo_ids: memberIds, burst_index: grmState.burstIdx }),
  });
  // Adopt authoritative structure (mirrors confirmSpecies()).
  if (resp && resp.encounters) {
    pipelineResults.encounters = resp.encounters;
    if (resp.summary) pipelineResults.summary = resp.summary;
  }
}
```

**Step 4: Reorder the body of `grmApply`**

Target sequence (replacing lines ~4818-4892):

1. `if (checks.flags)`: existing browse-selection branch + removed→detach branch
   run as today (they mutate `pipelineResults`). When flags unchecked, skip both.
2. Remove the old local `enc.species_confirmed = true` block (lines ~4872-4879) —
   confirmation is now server-owned via the helper.
3. `safeFetch('/api/pipeline/save-cache', ...)` with `pipelineResults` (as today),
   so the cache on disk is consistent before the species call reads it.
4. `if (checks.species)`: compute the **remaining** burst member ids
   `memberIds = grmState.items.filter(p => !grmState.removed.has(p.id)).map(p => p.id)`
   and `await grmConfirmSpeciesCall(species, memberIds);`
5. `closeGroupReview(); document.getElementById('grmSpecies').value = '';`
   `renderResults(); updateSummaryBar(refreshLocalSummaryCounts());`

Note the browse-selection branch currently early-returns (lines 4818-4852). Refactor
so that branch performs its `pipelineResults` mutations + save-cache, then falls
through to the shared species-confirm + close steps (do **not** early-return before
species confirm). Preserve its special "all removed → fallbackToNormalPipelineReview"
case (still early-returns after persisting, since there's no burst left to confirm).

**Step 5: Verify end-to-end in the app**

- Species-only (uncheck flags): flags unchanged in DB; all burst frames tagged;
  burst shows confirmed (grid badge agrees).
- Flags-only (uncheck species): flags land; species stays unconfirmed.
- Both: both land. Species replacement untags the old keyword.
- Removed photo + flags unchecked: removal is **not** applied (amber hint warned).

**Step 6: Commit**

```bash
git add vireo/templates/pipeline_review.html
git commit -m "pipeline review: grmApply commits only checked sides; species via unified path"
```

---

## Task 5: E2E coverage (Playwright, user-first)

**Files:**
- Modify/Create: `tests/e2e/test_pipeline_review_species.py` (extend existing
  species E2E) — or a new `tests/e2e/test_burst_group_confirm_split.py`.

Follow the existing E2E harness conventions in that directory (fixture that builds
test photos via `scripts/build_test_photos.py`, launches the app, drives a real
browser). See `tests/e2e/test_pipeline_review_species.py` and
`tests/e2e/test_pipeline_rapid_review.py` for the setup pattern.

**Step 1: Write the failing E2E tests**

Cases (one test function each):

1. `test_smart_default_flags_checked_when_moves_pending` — move a photo to
   rejects → "Apply picks/rejects" checkbox is checked, "Confirm species" not.
2. `test_smart_default_species_checked_on_new_species` — type a species differing
   from confirmed → "Confirm species" checks.
3. `test_unchecking_dirty_box_shows_amber_hint` — uncheck a checked, dirty box →
   the `.grm-dirty-hint` becomes visible with expected text.
4. `test_species_only_apply_leaves_flags_untouched` — uncheck flags, confirm
   species, Apply → assert via `/api/pipeline/results` or DB that flags are
   unchanged and all burst frames carry the species keyword.
5. `test_flags_only_apply_leaves_species_unconfirmed` — confirm flags only →
   species not confirmed (no keyword added, burst not confirmed).

**Step 2: Run to verify they fail / drive remaining gaps**

Run: `python -m pytest tests/e2e/test_burst_group_confirm_split.py -v`
Expected: tests written before/independthat catch regressions fail first, then
pass against the Task 2-4 implementation.

**Step 3: Make them pass**

Adjust selectors/assertions to the implemented markup; fix any real bug surfaced.

**Step 4: Commit**

```bash
git add tests/e2e/test_burst_group_confirm_split.py
git commit -m "test(e2e): burst-group confirm/apply split behaviors"
```

---

## Task 6: Full regression + PR

**Step 1: Run the CLAUDE.md test bundle**

Run:
```bash
python -m pytest tests/test_workspaces.py vireo/tests/test_db.py \
  vireo/tests/test_app.py vireo/tests/test_photos_api.py \
  vireo/tests/test_edits_api.py vireo/tests/test_jobs_api.py \
  vireo/tests/test_darktable_api.py vireo/tests/test_config.py \
  vireo/tests/test_pipeline_group_apply.py -v
```
Expected: PASS (ignore the known pre-existing failures noted in memory; confirm
none are newly introduced by this change).

**Step 2: Create the PR**

```bash
gh pr create --base main --title "Burst group: split species confirm from pick/reject apply" \
  --body "<what changed + test results, link design doc>"
```

**Step 3:** Push review-feedback fixes to the same branch.

---

## Notes / open verifications for the implementer

- **Browse-selection species:** `/api/batch/keyword` with `type:"taxonomy"` is the
  chosen path (taxonomy reconciles to `is_species` in `db.add_keyword`). Verify a
  brand-new species name not in any taxon list still lands as a species keyword
  (it should, via the explicit `taxonomy` type).
- **`detachNew` under "Apply picks/rejects":** removals are gated by the flags
  checkbox. If the user wants removals independent of flags, revisit — but the
  design treats both as "cull edits".
- **`grmComputeDiff` already excludes species when** `keywordStateSpecies !==
  species`; that staleness is fine for the checkbox (it only affects the
  `tagNew` count in the label, not the checked state which keys off
  `speciesChanged`).
