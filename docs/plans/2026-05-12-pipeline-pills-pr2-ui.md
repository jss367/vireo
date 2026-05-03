# Pipeline Pills PR #2 — UI Pill Formatter + Progress Bar

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Replace the static `_PILL_LABELS` lookup in `vireo/templates/pipeline.html` with a smart formatter that surfaces "Resume (N left)" / "Outdated (N to redo)" / counts, and add a 2-segment progress bar to the Classify / Extract / Eye Keypoints cards. PR #1 already shipped uniform `detail.pending` + `detail.eligible` across stage planners; this PR consumes them.

**Architecture:** All work is in one HTML template — `vireo/templates/pipeline.html`. Two new JS helpers (`_formatPillLabel`, `_renderProgressBar`), one new CSS rule block (`.stage-progress-bar`), and three small markup additions (one progress bar element per card header). No backend changes, no new endpoints. Verification is browser-based via Playwright walking the three headline scenarios.

**Tech Stack:** Vanilla JS (no framework), CSS in template `<style>` block, Playwright for browser testing.

**Reference design:** `docs/plans/2026-05-08-pipeline-status-makeover-phase2-design.md`.

**Key file lines (current state, captured pre-edit):**
- `_PILL_LABELS` map: `vireo/templates/pipeline.html:3470-3477`
- `_setPill` function: lines 3479-3484
- `_stageStateFor`: lines 3503-3520
- `refreshPipelineUI`: lines 3557-3568
- `_pipelinePlan` global: line 3440
- `_STAGES` definition: lines 3444-3451
- Pill CSS: lines 64-99
- Card markup — Classify: 899-941, Extract: 944-1001, EyeKeypoints: 1004-1030

**Project conventions:**
- User-first testing per `MEMORY.md`: drive a real browser via Playwright, walk through scenarios like a user, don't just read code.
- Run dev server: `python vireo/app.py --db ~/.vireo/vireo.db --port 8080`. Use a separate test DB / config to avoid polluting the dev's actual workspace.

---

## Task 1: CSS for the progress bar

**Files:** `vireo/templates/pipeline.html` lines 64–99 area (existing `.stage-status-pill` block)

**Step 1: Add new CSS rules immediately after the existing `.stage-status-pill.failed` rule** (around line 99). Keep them alphabetized after the pill rules.

```css
.stage-progress-bar {
  display: inline-flex;
  width: 80px;
  height: 6px;
  margin-left: 8px;
  border-radius: 3px;
  overflow: hidden;
  background: var(--bg-tertiary, #14374E);
  vertical-align: middle;
}
.stage-progress-bar.hidden { display: none; }
.stage-progress-bar-fill {
  background: var(--accent, #24E5CA);
  transition: width 200ms ease-out;
}
.stage-progress-bar.outdated .stage-progress-bar-fill {
  background: var(--warning, #F0A030);
}
```

The `--warning` CSS var likely doesn't exist yet — check by grepping `--warning` in pipeline.html. If absent, hardcode `#F0A030` (an amber that contrasts with the existing teal accent).

**Step 2: Manual smoke** — load the page in a browser, no markup added yet, confirm the page still renders unchanged. CSS without consumers is a no-op.

**Step 3: Commit**
```bash
git add vireo/templates/pipeline.html
git commit -m "pipeline-ui: add stage progress bar CSS"
```

---

## Task 2: Add progress bar markup to three cards

**Files:** `vireo/templates/pipeline.html`
- Classify card (`id="card-classify"`, lines ~899-941)
- Extract card (`id="card-extract"`, lines ~944-1001)
- Eye Keypoints card (`id="card-eyekeypoints"`, lines ~1004-1030)

**Step 1: For each of the three cards**, find the line that looks like:

```html
<span class="stage-status-pill" id="pillClassify"></span>
```

Insert a new element immediately after it:

```html
<span class="stage-status-pill" id="pillClassify"></span>
<div class="stage-progress-bar hidden" id="progressBarClassify">
  <div class="stage-progress-bar-fill" style="width:0%"></div>
</div>
```

Same pattern for `pillExtract` → `progressBarExtract`, `pillEyeKeypoints` → `progressBarEyeKeypoints`.

The `.hidden` class keeps the bar invisible until JS populates it (avoids a flash of empty bar on initial load).

**Step 2: Reload the page in a browser** — the bars should be invisible (hidden class), no other change. Just confirming markup didn't break layout.

**Step 3: Commit**
```bash
git add vireo/templates/pipeline.html
git commit -m "pipeline-ui: add progress bar element to classify/extract/eyekeypoints cards"
```

---

## Task 3: `_formatPillLabel(stage, planEntry)` helper

**Files:** `vireo/templates/pipeline.html` JS, around line 3479 (current `_setPill`)

**Step 1: Add a new helper above `_setPill`:**

```javascript
// Map a stage's plan entry to the pill text, including counts and the
// staleness signal. Returns the literal string to display.
//
// The plan endpoint emits `state: 'will-run'` for both fresh and
// outdated cases; the distinction lives in `detail.fingerprint_outdated`
// / `detail.fingerprint_invalidated`. We surface that here as a distinct
// "Outdated" label so users see settings-changed staleness directly,
// not buried in the summary text.
function _formatPillLabel(stage, planEntry, state) {
  // Special-cased states (running/done/failed/will-skip) have static
  // labels — counts during a run come from a separate channel.
  if (state === 'running')   return 'Running…';
  if (state === 'done')      return 'Done';
  if (state === 'failed')    return 'Failed';
  if (state === 'will-skip') return 'Will skip';

  if (!planEntry) {
    // No plan entry yet (page still loading) — fall back to the bare
    // state label. Same behavior as the pre-PR-#2 lookup.
    return state === 'done-prior' ? 'Already done' : 'Will run';
  }
  var detail   = planEntry.detail || {};
  var pending  = (typeof detail.pending  === 'number') ? detail.pending  : null;
  var eligible = (typeof detail.eligible === 'number') ? detail.eligible : null;
  var outdated = !!(detail.fingerprint_outdated || detail.fingerprint_invalidated);

  // No quantifiable work — fall back to the state-only label.
  if (eligible === null || eligible === 0) {
    return state === 'done-prior' ? 'Already done' : 'Will run';
  }

  if (state === 'done-prior') {
    return 'Already done (' + eligible + ')';
  }

  // state === 'will-run' from here on.
  if (outdated)                  return 'Outdated (' + (pending || eligible) + ' to redo)';
  if (pending !== null && pending > 0 && pending < eligible) {
    return 'Resume (' + pending + ' left)';
  }
  return 'Will run (' + eligible + ')';
}
```

**Step 2: Update `_setPill` to use the formatter:**

Replace:
```javascript
function _setPill(suffix, state, text) {
  var pill = document.getElementById('pill' + suffix);
  if (!pill) return;
  pill.className = 'stage-status-pill visible ' + state;
  pill.textContent = text != null ? text : _PILL_LABELS[state];
}
```

with:
```javascript
function _setPill(suffix, state, text, stage) {
  var pill = document.getElementById('pill' + suffix);
  if (!pill) return;
  pill.className = 'stage-status-pill visible ' + state;
  if (text != null) {
    pill.textContent = text;
    return;
  }
  var planEntry = _pipelinePlan && _pipelinePlan.stages
    ? _pipelinePlan.stages[suffix] : null;
  pill.textContent = _formatPillLabel(stage, planEntry, state);
}
```

**Step 3: Update `refreshPipelineUI` to pass `stage` to `_setPill`:**

Replace:
```javascript
_setPill(stage.suffix, state);
```

with:
```javascript
_setPill(stage.suffix, state, null, stage);
```

**Step 4: Audit other call sites of `_setPill`** — grep for `_setPill(` in pipeline.html. There may be call sites in SSE event handlers (e.g., when a stage transitions to running/done/failed). Those pass an explicit text, so they're unaffected (the `text != null` shortcut returns early). Just confirm no caller breaks from the new optional `stage` parameter.

**Step 5: Browser smoke** — load the page on a real workspace with some prior data. The pill text should now say "Already done (N)" instead of "Already done", and "Will run (N)" instead of "Will run". On a stage that's been partially classified, the pill should say "Resume (N left)".

If the user has changed grouping settings since the last regroup, the Group pill should still say "Will run" (Group has no per-photo unit, so falls back to bare label). The next task adds the bar — pill counts work even without it.

**Step 6: Commit**
```bash
git add vireo/templates/pipeline.html
git commit -m "pipeline-ui: pill formatter surfaces counts and outdated state"
```

---

## Task 4: `_renderProgressBar(stage, planEntry)` helper

**Files:** `vireo/templates/pipeline.html` JS

**Step 1: Add a new helper above `refreshPipelineUI`:**

```javascript
// Stages that don't have a per-photo work unit don't get a progress bar.
// Group is workspace-level; Scan/Previews don't have stage cards.
var _STAGES_WITH_BAR = { 'Classify': 1, 'Extract': 1, 'EyeKeypoints': 1 };

function _renderProgressBar(stage, planEntry) {
  var bar = document.getElementById('progressBar' + stage.suffix);
  if (!bar) return;
  if (!_STAGES_WITH_BAR[stage.suffix]) {
    bar.classList.add('hidden');
    return;
  }
  var detail   = (planEntry && planEntry.detail) || {};
  var pending  = (typeof detail.pending  === 'number') ? detail.pending  : null;
  var eligible = (typeof detail.eligible === 'number') ? detail.eligible : null;
  if (eligible === null || eligible === 0) {
    bar.classList.add('hidden');
    return;
  }
  bar.classList.remove('hidden');
  var outdated = !!(detail.fingerprint_outdated || detail.fingerprint_invalidated);
  bar.classList.toggle('outdated', outdated);
  var done = Math.max(0, eligible - (pending || 0));
  var pct = (done / eligible) * 100;
  var fill = bar.querySelector('.stage-progress-bar-fill');
  if (fill) fill.style.width = pct + '%';
}
```

**Step 2: Wire into `refreshPipelineUI`:**

Replace:
```javascript
function refreshPipelineUI() {
  _STAGES.forEach(function(stage) {
    var state = _stageStateFor(stage);
    _setPill(stage.suffix, state, null, stage);
    if (!_runningStages[stage.suffix] && !_stageOutcomes[stage.suffix]) {
      _setStageSummaryText(stage.suffix, _stageSummaryFor(stage));
    }
  });
  _renderPlanSummary();
}
```

with:
```javascript
function refreshPipelineUI() {
  _STAGES.forEach(function(stage) {
    var state = _stageStateFor(stage);
    _setPill(stage.suffix, state, null, stage);
    var planEntry = _pipelinePlan && _pipelinePlan.stages
      ? _pipelinePlan.stages[stage.suffix] : null;
    _renderProgressBar(stage, planEntry);
    if (!_runningStages[stage.suffix] && !_stageOutcomes[stage.suffix]) {
      _setStageSummaryText(stage.suffix, _stageSummaryFor(stage));
    }
  });
  _renderPlanSummary();
}
```

**Step 3: Browser smoke** — same as Task 3 but now bars should appear:
- Empty workspace (no detections, no masks): bars hidden (`eligible == 0`).
- Detections cached, no classify done: full empty bar.
- Some classify done, some pending: partial green fill matching `done / eligible`.
- Settings change so fingerprint mismatches: amber fill for the outdated stage.

**Step 4: Commit**
```bash
git add vireo/templates/pipeline.html
git commit -m "pipeline-ui: 2-segment progress bar on classify/extract/eyekeypoints cards"
```

---

## Task 5: Browser verification (Playwright)

**Files:** none — this is manual verification, captured in PR description.

The project's `MEMORY.md` records "user-first testing" as a convention: when testing UI work, drive a real browser and interact like a user. We walk three scenarios.

**Setup:** Use a temp DB so the dev's actual workspace isn't touched.

```bash
mkdir -p /tmp/vireo-pr2-test
python vireo/app.py --db /tmp/vireo-pr2-test/vireo.db --port 8090 &
APP_PID=$!
# wait a beat, then drive Playwright at http://localhost:8090
```

Three scenarios to drive (use Playwright MCP if available, else screenshots from a manual browser session).

### Scenario A — empty workspace

1. Visit `/pipeline`.
2. Pills should show "Will skip" / "Will run" without counts (no eligible items yet).
3. Progress bars hidden.
4. Capture screenshot.

### Scenario B — mid-run cancel → resume

Hard to fake without real photos. Substitute: ingest 5 sample photos (any wildlife images), run classify on 2 of them via DB inserts, then refresh the page.

1. Pills should show:
   - Classify: "Resume (3 left)" with bar partially filled green.
2. Capture screenshot.

If hand-driving via DB inserts is too fiddly, skip this and rely on the API-level test that PR #1 already covers; document the skip in the PR description.

### Scenario C — settings changed after a clean run

1. Set up a workspace where eye keypoints have been computed (DB rows with `eye_kp_fingerprint = 'v1'`).
2. Bump `EYE_KP_FINGERPRINT_VERSION` to `'v2'` in `vireo/pipeline.py`.
3. Reload the pipeline page.
4. Eye Keypoints pill should show "Outdated (N to redo)" with amber fill.
5. Restore the constant to `'v1'` and reload — pill should go back to "Already done (N)".
6. Capture screenshots before / after.

**Step 1: Capture before / after screenshots for each scenario into `/tmp/pr2-screenshots/`**.

**Step 2: Add a "Test plan" section to the PR description** that describes what was driven and links the screenshots (or pastes them inline).

---

## Task 6: Push branch + open PR

**Step 1: Run focused project tests for regression check**
```bash
python -m pytest vireo/tests/test_pipeline_plan.py vireo/tests/test_app.py vireo/tests/test_pipeline.py -v
```

Expected: all pass. (We didn't touch backend, so this is just a sanity check that we didn't accidentally edit a non-template file.)

**Step 2: Push and open PR**
```bash
git push -u origin claude/pipeline-pills-ui
gh pr create --base main --title "pipeline-ui: pill counts + progress bars on stage cards" --body "$(cat <<'EOF'
## Summary

Phase 2 PR #2 of the pipeline status makeover ([phase 2 design](docs/plans/2026-05-08-pipeline-status-makeover-phase2-design.md), [PR2 plan](docs/plans/2026-05-12-pipeline-pills-pr2-ui.md)). UI-only; reads the uniform \`detail.pending\` / \`detail.eligible\` fields shipped in PR #747.

- Pill labels now surface counts and the staleness signal:
  - \"Will run (1500)\" / \"Resume (266 left)\" / \"Outdated (266 to redo)\" / \"Already done (1500)\"
- New 2-segment progress bar on Classify / Extract / Eye Keypoints cards. Green when fresh, amber when fingerprint outdated. Hidden when no quantifiable work.
- Group stays text-only (no per-photo unit).

## Test plan
- [x] Three browser scenarios driven via Playwright: empty workspace, partial completion, settings-change. Screenshots inline below.
- [x] Backend regression check: \`test_pipeline_plan.py\` + \`test_app.py\` + \`test_pipeline.py\` green.

[Screenshots inserted from /tmp/pr2-screenshots/]

🤖 Generated with [Claude Code](https://claude.com/claude-code)
EOF
)"
```

---

## Out of scope

- Live SSE counts in pills during runs ("Running… 234 / 1500"). Users still get live progress from existing `.progress-text`.
- Provenance line ("Last run 2h ago · model X").
- Removing legacy `has_detections` / `has_masks` / `has_sharpness` consumers.
- Adding "Done (N)" pill state for just-completed stages — currently "Done", with no count.
