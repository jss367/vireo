# Pipeline Pills PR #4 — Live SSE counts + legacy `has_*` cleanup Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Surface live SSE progress counts inside the running-stage pill ("Running… 234 / 1500"), and delete the legacy `has_detections` / `has_masks` / `has_sharpness` fields from `/api/pipeline/page-init` along with their consumers in `pipeline.html`.

**Architecture:** Two narrow commits. (A) Single hook in `_updatePipelineStageUI(p)` overrides the running pill's text when the SSE event carries `current/total`. (C) Producer-side delete in `app.py` + helper delete in `db.py` + ~10 consumer lines deleted in `pipeline.html` + 3 test assertion lines deleted in `test_app.py`.

**Tech Stack:** Vanilla JS, Flask, SQLite, pytest. Browser smoke via Playwright.

**Reference design:** `docs/plans/2026-05-15-pipeline-pills-pr4-sse-and-cleanup-design.md`.

**Key file lines (current state, captured pre-edit):**
- `_updatePipelineStageUI`: `vireo/templates/pipeline.html:2522`
- Existing `_setPill(suffix, 'running')` call: line 2555
- `txtDetections` / `txtMasks` span definitions: lines 928, 976
- `has_*` consumers in JS: lines 1141, 1146-1149, 1153-1157, 1184-1185
- `_pipelineState.hasDetections / hasMasks`: setters at 2724, 2739, 3142, 3410; reader at 3758; key declaration at 3442-3443
- Auto-expand-extract behavior: line 3758 inside `updateCardStates`
- Post-run text updates (NOT to be deleted — they keep the summary span fresh after job completion): lines 2723, 2738, 3141, 3408
- Producer `app.py:1166, 1206-1208` (`get_pipeline_feature_counts` call + 3 jsonify keys)
- Helper `db.py:2699-2733` (`get_pipeline_feature_counts`)
- Test assertions `test_app.py:1068-1070`

**Wrinkle:** `txtDetections` / `txtMasks` span elements stay — they're also used as stage-summary spans (`_STAGE_SUMMARY_IDS` map at line 3568-3573) for the plan endpoint's summary text. We just stop the legacy `has_*` initialization from writing to them.

**Test command:**
```bash
python -m pytest vireo/tests/test_app.py vireo/tests/test_db.py -v
```

---

## Task 1 — Live SSE counts in pills

**Files:**
- Modify: `vireo/templates/pipeline.html` around line 2555 (inside `_updatePipelineStageUI`)

**Step 1: Read the existing transition block**

Find this block in `_updatePipelineStageUI`:

```javascript
if (a.running) {
  numEl.className = 'stage-num running';
  _runningStages[cardSuffix2] = true;
  _setPill(cardSuffix2, 'running');
  var card = document.getElementById('card-' + cardSuffix2.toLowerCase());
  if (card) card.classList.add('expanded');
}
```

**Step 2: Add a sibling block right after it that overrides the pill text when SSE carries `current/total`**

Replace the block with:

```javascript
if (a.running) {
  numEl.className = 'stage-num running';
  _runningStages[cardSuffix2] = true;
  _setPill(cardSuffix2, 'running');
  // Override pill with the live X/Y count when this SSE event carries
  // them. Stages that emit progress without counts (e.g. spin-up phases,
  // Group's terminal step) keep the static "Running…" label rather than
  // showing a misleading "Running… 0 / 0".
  if (typeof p.current === 'number' && typeof p.total === 'number'
      && p.total > 0) {
    _setPill(cardSuffix2, 'running',
             'Running… ' + p.current + ' / ' + p.total);
  }
  var card = document.getElementById('card-' + cardSuffix2.toLowerCase());
  if (card) card.classList.add('expanded');
}
```

**Step 3: Smoke verification (template syntax only — full browser test is Task 3)**

```bash
python -c "
from jinja2 import Environment, FileSystemLoader
env = Environment(loader=FileSystemLoader('vireo/templates'))
env.get_template('pipeline.html')
print('template parses OK')
"
```

Expected: `template parses OK`.

**Step 4: Commit**

```bash
git add vireo/templates/pipeline.html
git commit -m "pipeline-ui: live SSE counts in running-stage pill

The running pill stays generic ('Running…') even though every progress
event already carries current/total. Hook the override into the
existing transition block in _updatePipelineStageUI so the pill becomes
'Running… 234 / 1500' for stages that emit count data, and falls back
to the static label otherwise.

One conditional, ~5 lines, reuses _setPill's existing text-override
path. Group + Eye Keypoints stay 'Running…' because they don't emit
counts today — accurate, not misleading."
```

---

## Task 2 — Legacy `has_*` cleanup

**Files:**
- Modify: `vireo/app.py:1162-1217` (the `/api/pipeline/page-init` route + jsonify response)
- Delete from: `vireo/db.py:2699-2733` (`get_pipeline_feature_counts` method)
- Modify: `vireo/templates/pipeline.html` (10 consumer references)
- Modify: `vireo/tests/test_app.py:1068-1070` (3 assertions)

**Step 1: Drop producer in `app.py`**

In `vireo/app.py`, find the function `api_pipeline_page_init` (around line 1162). Make these changes:

- Delete line 1166: `pipeline_counts = db.get_pipeline_feature_counts()`
- Delete lines 1206-1208 (the 3 `has_detections` / `has_masks` / `has_sharpness` keys in the `jsonify` block).

**Step 2: Delete the helper in `db.py`**

In `vireo/db.py`, find `def get_pipeline_feature_counts(self):` (around line 2699). Delete the entire method including its docstring (~35 lines). The method ends right before the next `def` at line 2733-ish.

Verify no remaining callers:
```bash
grep -rn "get_pipeline_feature_counts" /Users/julius/conductor/workspaces/vireo/charlotte/vireo
```
Expected: no output (after this delete).

**Step 3: Drop test assertions**

In `vireo/tests/test_app.py`, find `def test_pipeline_page_init_api` (around line 1060). Delete lines 1068-1070:
```python
assert 'has_detections' in data
assert 'has_masks' in data
assert 'has_sharpness' in data
```

Keep the rest of the test — `total_photos`, `taxonomy_available`, etc., still apply.

**Step 4: Run tests, expect green**

```bash
python -m pytest vireo/tests/test_app.py::test_pipeline_page_init_api vireo/tests/test_db.py -v
```

Expected: PASS — the producer and helper are gone, the test no longer asserts on the dropped fields, and `db.py` tests don't reference the helper.

**Step 5: Drop consumers in `pipeline.html`**

Open `vireo/templates/pipeline.html`. Make these deletions:

(a) **Source / Classify / Extract complete badges + initial summary text** — delete lines 1141-1158 (the entire block reading `data.has_detections / has_masks / has_sharpness` and writing to `numSource`/`numClassify`/`numExtract` complete badges + `txtDetections`/`txtMasks` text).

The block looks like:
```javascript
if (data.has_detections || data.has_masks || data.results) {
  document.getElementById('numSource').classList.add('complete');
}
if (data.has_detections) {
  document.getElementById('numClassify').classList.add('complete');
  document.getElementById('txtDetections').textContent =
    data.has_detections + ' detections';
}
if (data.has_masks) {
  document.getElementById('numExtract').classList.add('complete');
  var txt = data.has_masks + ' masks';
  if (data.has_sharpness) txt += ', ' + data.has_sharpness + ' sharpness';
  document.getElementById('txtMasks').textContent = txt;
}
```

Delete the entire block.

(b) **State hydration** — delete lines 1184-1185:
```javascript
_pipelineState.hasDetections = !!data.has_detections;
_pipelineState.hasMasks = !!data.has_masks;
```

(c) **State setters after job completion** — delete each of these lines (the `_pipelineState.hasDetections/.hasMasks = true` setters; the surrounding `txtDetections.textContent = ...` lines STAY because they update the stage-summary span with run results):
- Line 2724: `_pipelineState.hasDetections = true;`
- Line 2739: `_pipelineState.hasMasks = true;`
- Line 3142: `_pipelineState.hasDetections = true;`
- Line 3410: `_pipelineState.hasMasks = true;`

(d) **Auto-expand-extract reader** — delete line 3758. Find the block in `updateCardStates`:
```javascript
if (_pipelineState.hasDetections && !_pipelineState.hasMasks) {
  document.getElementById('card-extract').classList.add('expanded');
}
```
Delete the entire `if` block.

(e) **State key declarations** — delete lines 3442-3443:
```javascript
hasDetections: false,
hasMasks: false,
```

If `_pipelineState` becomes empty after, leave the empty object in place (less churn).

**Step 6: Verify no remaining references**

```bash
grep -n "has_detections\|has_masks\|has_sharpness\|hasDetections\|hasMasks" \
  /Users/julius/conductor/workspaces/vireo/charlotte/vireo/templates/pipeline.html \
  /Users/julius/conductor/workspaces/vireo/charlotte/vireo/app.py \
  /Users/julius/conductor/workspaces/vireo/charlotte/vireo/db.py \
  /Users/julius/conductor/workspaces/vireo/charlotte/vireo/tests/test_app.py
```

Expected: no output.

Also verify the template still parses:
```bash
python -c "
from jinja2 import Environment, FileSystemLoader
env = Environment(loader=FileSystemLoader('vireo/templates'))
env.get_template('pipeline.html')
print('template parses OK')
"
```

**Step 7: Run focused test suite for regression**

```bash
python -m pytest vireo/tests/test_app.py vireo/tests/test_db.py vireo/tests/test_pipeline_plan.py -v
```

Expected: all PASS.

**Step 8: Commit**

```bash
git add vireo/app.py vireo/db.py vireo/templates/pipeline.html vireo/tests/test_app.py
git commit -m "pipeline: drop legacy has_* fields, superseded by /api/pipeline/plan

has_detections / has_masks / has_sharpness pre-date PR #745's plan
endpoint. They fed three minor side-effects in pipeline.html:
- Source / Classify / Extract card 'complete' badges
- Initial summary text in txtDetections / txtMasks spans
- An auto-expand-extract behavior in updateCardStates

All redundant now. The pill (PR #748 formatter, PR #749 outdated flags)
and progress bar carry the same signals more accurately, and the plan
summary text writes to the same spans via _STAGE_SUMMARY_IDS.

Deletes:
- /api/pipeline/page-init: 3 keys + the unused get_pipeline_feature_counts() call
- db.get_pipeline_feature_counts (sole caller is gone)
- pipeline.html: ~12 consumer lines + _pipelineState.hasDetections/hasMasks
- test_app.py: 3 assertion lines

The txtDetections / txtMasks span elements stay — they're the
stage-summary spans the plan summary text writes to via
_STAGE_SUMMARY_IDS. The post-run setters that update them with run
results also stay."
```

---

## Task 3 — Browser smoke (driven by lead, not subagent)

**This is lead-driven.** The lead validates both commits in a real browser:

1. Start an isolated dev server:
```bash
mkdir -p /tmp/vireo-pr4-test
HOME=/tmp/vireo-pr4-test python vireo/app.py --db /tmp/vireo-pr4-test/vireo.db --port 8090 &
```

2. **Task A verification** — drive a real (or simulated) SSE event flow and capture the pill mid-run. Cleanest path: use Playwright `page.evaluate` to call `_updatePipelineStageUI({stages: [{suffix: 'Classify', running: true}], current: 234, total: 1500})` directly. Capture screenshot; pill should read "Running… 234 / 1500".

3. **Task C verification** — load `/pipeline` in the browser. Confirm:
   - Page renders without console errors.
   - Pills + progress bars + plan summary all work.
   - The card "complete" badges (small visual checkmark on Source/Classify/Extract circles) are gone — that's the expected behavior loss.
   - `txtDetections` / `txtMasks` spans show plan-endpoint summary text (e.g., "Already classified — 100 detections across 1 model"), not legacy "X detections" / "X masks" text.

Capture screenshots into `/tmp/pr4-screenshots/` and inline them in the PR description.

---

## Task 4 — Push + open PR

**Step 1: Run focused test suite**

```bash
python -m pytest tests/test_workspaces.py vireo/tests/test_db.py vireo/tests/test_app.py vireo/tests/test_photos_api.py vireo/tests/test_edits_api.py vireo/tests/test_jobs_api.py vireo/tests/test_darktable_api.py vireo/tests/test_config.py vireo/tests/test_pipeline.py vireo/tests/test_pipeline_job.py vireo/tests/test_pipeline_plan.py
```

Triage: 2 pre-existing keyword failures in `test_edits_api.py` per `MEMORY.md` are OK; anything else is on us.

**Step 2: Push and open PR**

```bash
git push -u origin claude/pipeline-pills-sse-cleanup
gh pr create --base main --title "pipeline-ui: live SSE pill counts + legacy has_* cleanup" --body "$(cat <<'EOF'
## Summary

PR #4 of the pipeline status makeover ([design](docs/plans/2026-05-15-pipeline-pills-pr4-sse-and-cleanup-design.md)). Two narrow commits.

**Commit 1 — Live SSE counts in pills.** A 5-line hook in \`_updatePipelineStageUI\` overrides the running pill's text to "Running… 234 / 1500" when the SSE event carries \`current\`/\`total\`. Stages that don't emit counts (Group, Eye Keypoints today) keep the static "Running…" label rather than showing a misleading "Running… 0 / 0". Reuses \`_setPill\`'s existing text-override path; no new infrastructure.

**Commit 2 — Drop legacy \`has_*\` fields.** \`has_detections\` / \`has_masks\` / \`has_sharpness\` pre-date PR #745's \`/api/pipeline/plan\` endpoint. They fed three minor side-effects in \`pipeline.html\` (card "complete" badges, initial summary text in \`txtDetections\`/\`txtMasks\` spans, an auto-expand-extract behavior). All redundant now — the pill and progress bar from PR #748/#749 carry the same signals more accurately, and the plan endpoint's per-stage summary text writes to the same spans.

The \`txtDetections\` / \`txtMasks\` span elements stay — they're the stage-summary spans the plan summary writes to via \`_STAGE_SUMMARY_IDS\`. Post-run setters that update them with run results also stay.

## Test plan
- [x] Browser smoke via Playwright on isolated dev server: pill mid-run shows "Running… 234 / 1500"; \`/pipeline\` page renders cleanly without legacy \`has_*\` references; pills + progress bars + plan summary all work.
- [x] Focused project test suite green minus the 2 pre-existing keyword-edit failures from \`MEMORY.md\`.

🤖 Generated with [Claude Code](https://claude.com/claude-code)
EOF
)"
```

---

## Out of scope

- Provenance line per card.
- Throttling SSE pill updates (browser DOM updates are cheap; revisit only if observed).
- Keeping the auto-expand-extract behavior — pill carries the same signal, drop is fine.

This is the closing PR of the pipeline status makeover. Headline UX is now end-to-end: cancel-and-resume, settings-changed staleness, count surfaces, amber bar, and live counts during runs all work.
