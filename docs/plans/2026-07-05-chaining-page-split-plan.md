# Chaining + Page Split Implementation Plan (import/process split PR 3)

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Wire import → process chaining ("After import: <strategy>") and split the pipeline wizard into an Import page and a Process page, so the two jobs shipped in PR 1 (#1103) and PR 2 (#1107) become the user-facing workflow.

**Architecture:** The chaining hook lives in the import job's completion path in `app.py` (not in `run_import_job` — the job stays pure). A shared enqueue helper factored out of `api_job_pipeline` gives the hook and the route identical semantics (strategy expansion, no-model auto-skip, runtime warnings). The Import page is a new `import.html` + `/import` route calling `POST /api/jobs/import-photos`; the pipeline page loses its "Import Photos" source and becomes the Process page. UI work follows the CORE_PHILOSOPHY transparency rule throughout: every pill answers the question the user reads it as.

**Tech Stack:** Python 3, Flask, Jinja2 + vanilla JS (inline per-page CSS/JS), pytest, Playwright for the user-first scenario.

**Design doc:** `docs/plans/2026-07-04-import-process-split-design.md` (#1101)
**Parent plan:** `docs/plans/2026-07-04-import-process-split-plan.md` (#1102) — Phase 3 skeleton
**Predecessors:** PR 1 #1103 (strategies, folder scope, workspace default), PR 2 #1107 (import job; Task 2.7 remote destinations deferred separately)

**Test command (run before the PR):**
```bash
python -m pytest tests/test_workspaces.py vireo/tests/test_db.py vireo/tests/test_app.py vireo/tests/test_photos_api.py vireo/tests/test_edits_api.py vireo/tests/test_jobs_api.py vireo/tests/test_darktable_api.py vireo/tests/test_config.py vireo/tests/test_process_strategies.py vireo/tests/test_pipeline_job.py vireo/tests/test_import_job.py -v
```

**Key existing code (verified 2026-07-05):**
- `vireo/import_job.py:1749` — `run_import_job` result: `{discovered, copied, verified, skipped_duplicate, failed, safe_to_format, unsafe_files, folders, cancelled, discovery_errors, ok, errors}`. **No imported photo ids** — Task 3.1a adds them; the chaining hook needs them for the process job's ad-hoc collection.
- `vireo/app.py:15006` — `api_job_import_photos` builds the `work` fn that calls `run_import_job`; the hook wraps this.
- `vireo/app.py:16463+` — `api_job_pipeline`: strategy expansion (PR 1), folder-scope collection creation, the no-model auto-skip + `model_warning`, `runner.enqueue_pipeline`. Task 3.1b factors the enqueue tail into a helper both callers share.
- `vireo/db.py:213` — `DEFAULT_TABS` (includes `"pipeline"`); `workspaces.tabs` JSON column; the 2026-04-30 unified-tabs migration pattern (reset in place, solo-user).
- `vireo/templates/pipeline.html` (~5,700 lines) — Stage-1 source cards: Import Photos (`radioImport`, :729), Use Existing Collection (`radioCollection`, :790), New images (:810, snapshot-backed per the 2026-04-22 design).
- `pipeline.default_strategy` (nullable) from PR 1; `after_import` on the import job config from PR 2, already validated at enqueue and defaulted from the workspace value.

---

## Task 3.0: Reconnaissance (no code changes)

Record answers in the Task 3.1 commit message or code comments:

1. **Tab registry**: where `_navbar.html` maps tab ids → labels/URLs, and what adding an `import` tab entails. Confirm whether tab ids appear anywhere besides `DEFAULT_TABS`, the navbar map, and workspace `tabs` rows.
2. **Readiness endpoints**: which endpoints the pipeline wizard already uses to render "Already done / Will run" per stage given current selections (the transparency rule requires reusing these, not `COUNT(*) > 0` proxies) — grep `readiness` / `already` in `pipeline.html` and `app.py`.
3. **Import-source UI pieces**: how the wizard's Import Photos path builds its source picker, destination + recent-destinations dropdown, and duplicate preview (`/api/import/check-duplicates`) — these move to `import.html` mostly intact.
4. **How `enqueue_pipeline` differs from `runner.start`** (slot queueing) — the chained process job must queue exactly like a user-started pipeline run.

## Task 3.1: Import → process chaining

**Files:**
- Modify: `vireo/import_job.py` (3.1a), `vireo/app.py` (3.1b, 3.1c)
- Test: `vireo/tests/test_import_job.py`, `vireo/tests/test_jobs_api.py`

**3.1a — result carries imported photo ids.**
- *Failing test:* import a fixture card; assert `result["photo_ids"]` is exactly the set of cataloged photo ids for the copied files (and stays present-but-empty on a duplicates-only run).
- *Implement:* `run_import_job` already observes `(photo_id, path)` via `photo_callback` during batch scans — accumulate and return them. Cap nothing; ids are ints.
- *Commit:* `feat: import result carries imported photo ids`

**3.1b — shared process-enqueue helper.**
- *Failing test:* API-level parity — `POST /api/jobs/pipeline {collection_id, strategy}` behavior is unchanged (reuse existing strategy tests as the regression net; add one asserting `model_warning` still surfaces).
- *Implement:* factor the tail of `api_job_pipeline` (params build from an expanded body, no-model auto-skip, runtime warning, `enqueue_pipeline`, job_config) into `_enqueue_process_job(db, runner, *, collection_id, strategy_name, body_overrides=None)` returning `(job_id, model_warning)`. The route becomes validation + call; the hook calls the same helper — one vocabulary, one auto-skip.
- *Commit:* `refactor: shared process-job enqueue helper`

**3.1c — the completion hook.**
- *Failing tests* (jobs API level, using `wait_for_job_via_client`):
  - `after_import: "cull_ready"` + successful import → a second job appears in history with `strategy == "cull_ready"` and a `collection_id` whose photos are exactly the import's `photo_ids`; the import job's result gains `process_job_id`.
  - `after_import: null` → exactly one job; import result records `after_import_skipped: "import-only"`; no failed follow-up anywhere (the parent plan's contract: the "no process" case never reaches `/api/jobs/pipeline`).
  - Import with a failed file (`ok: false`) → **no chaining** even when `after_import` is set; result records `after_import_skipped: "import failed"`. Processing a partial import automatically would hide the failure behind a green processing run — the user retries the import first (rollup-failure convention).
  - Duplicates-only import (0 copied, all verified duplicates, `ok: true`) → chaining still fires but scoped to... nothing new. Assert the hook skips with `after_import_skipped: "no new photos"` instead of enqueueing an empty run.
  - Chained enqueue with no model available → import result records both `process_job_id` **and** `model_warning` (same string the manual `/api/jobs/pipeline` route surfaces), so the jobs panel shows the no-model skip on the chained run too — not just on manually-started ones.
- *Implement:* in the route's `work` wrapper, after `run_import_job` returns: resolve the choice (job config `after_import`, already defaulted from the workspace at enqueue), apply the three skip guards (null / not ok / empty photo_ids), else create collection `"Import <YYYY-MM-DD HH:MM>"` from `photo_ids` and call `_enqueue_process_job`. Record `process_job_id` **and** `model_warning` (the second element of the helper's return tuple, when non-null) or `after_import_skipped` in the result — the jobs panel shows exactly what happened and why (transparency rule). Dropping `model_warning` here would make the chained no-model case look like a normal import in history while the same code path surfaces it via the manual route.
- *Commit:* `feat: import completion enqueues the after-import process job`

## Task 3.2: Import page

**Files:**
- Create: `vireo/templates/import.html`
- Modify: `vireo/app.py` (add `/import` route), `vireo/templates/_navbar.html`
- Test: `vireo/tests/test_app.py` (route + template smoke tests, mirroring existing page tests)

**Step 1: Failing tests** — `GET /import` returns 200 with the navbar and the after-import selector; the selector's default reflects the active workspace's `pipeline.default_strategy`; the page posts to `/api/jobs/import-photos` (assert the form/JS wiring by id, the way existing page tests assert `bp-compact-job` etc.).

**Step 2–3:** Extract the wizard's Import Photos path into `import.html`: source picker, destination + recents, folder template, duplicate preview (`/api/import/check-duplicates` — the preview and the gate share `import_dedup`, so the preview stays truthful), "After import" menu (`full` / `cull_ready` / `quick_look` / "None — import only" mapping to `null`), per-folder progress (see below), safe-to-format pill (`safe_to_format` + `unsafe_files` with reasons — render the reasons, not just a boolean), and a completion state listing the folders that were created with links into Browse. Keep the wizard untouched in this task — the Import page ships alongside it; the wizard loses its import source in Task 3.3.

**Live per-folder progress requires a backend widen.** `run_import_job._emit(phase, current, total, current_file)` today carries no per-folder shape, and `result.folders` (the terminal `folder_counts`) only lands on JobRunner's `complete` event — so the SSE stream alone can only render per-folder counts after the run finishes. Task 3.2 grows `_emit` to carry a `folders` dict of `{path: {copied, skipped, failed}}` (or the running counters keyed by folder), threads it through the import loop's per-file completion sites, and lets the Import page read it off the progress event. Assert it: an in-flight progress event mid-run already shows nonzero counts for the folder currently being copied — not just at completion. If a truthful live source can't be shipped in this task, the page must render an aggregate "N of M files copied" from `current`/`total` and defer the per-folder breakdown to the completion state — never fake per-folder progress from stale counters (transparency rule).

**Step 4–5:** Tests pass; commit — `feat: Import page`

## Task 3.3: Process page

**Files:**
- Modify: `vireo/templates/pipeline.html`, `vireo/app.py` (if readiness endpoints need a folder-scope variant)
- Test: `vireo/tests/test_app.py`

**Step 1: Failing tests** — `GET /pipeline` no longer contains the Import Photos source card (`radioImport` gone); it offers Folders / Collection / New images scopes and a strategy menu; posting builds `/api/jobs/pipeline` bodies with `folder_ids` **or** `collection_id` + `strategy` **or** `source_snapshot_id` + `strategy` (the New images scope; the wizard mints the snapshot via `POST /api/workspaces/active/new-images/snapshot` today at `pipeline.html:3086` and posts `body.source_snapshot_id` — the Process page keeps that same flow). Cover each of the three scopes in the failing test set; `_enqueue_process_job` must accept all three body shapes unchanged, or the New images scope silently 400s or (worse) runs against the wrong collection.

**Step 2–3:** Remove the import source path from the wizard (UI only — the `/api/jobs/pipeline` import mode keeps working until PR 4). Add the folder-scope picker (workspace folder tree, PR 1's `folder_ids`), keep the existing New images scope (snapshot mint on select, `source_snapshot_id` on submit), and the strategy menu (default from workspace). Per-stage rows keep the existing readiness endpoints; where a pill's meaning changes because import is gone (e.g. "source" language), reword to scope language. Page title/heading becomes "Process".

**Step 4–5:** Tests pass; commit — `feat: pipeline wizard becomes the Process page`

## Task 3.4: Tabs

**Files:**
- Modify: `vireo/db.py` (`DEFAULT_TABS` + `ALL_NAV_IDS` + one-time in-place update), `vireo/app.py` (`ALL_PAGES`), `vireo/templates/_navbar.html` (label map + the `window.NAV_ALL_PAGES` mirror)
- Test: `vireo/tests/test_db.py`, `vireo/tests/test_app.py`, `tests/test_workspaces.py`

Deviation from the parent plan's wording ("replace the `pipeline` tab with `import` + `process`"): the tab **id** `pipeline` stays (it is the URL and appears in every workspace's `tabs` JSON); its **label** becomes "Process", and a new `import` tab id + `/import` route is added ahead of it. Renaming the id would touch every deep link and saved tabs row for zero user-visible gain — label + new tab delivers the design's UX. Record this in the commit message.

**Nav registries — add `import` to every one, or the tab is silently dropped.** `Database.get_tabs()` filters saved `tabs` rows against `ALL_NAV_IDS` (unknown ids are dropped, per the "pages retired in past releases" comment) and `set_tabs()` raises on any id not in the set; `/api/workspace/tabs` returns `ALL_PAGES` (label/href/icon), which the client trusts as its registry (`_navbar.html` replaces its in-memory `ALL_PAGES` from `state.all_pages` on load). The `NAV_ALL_PAGES` block inline in `_navbar.html` is the pre-hydration fallback and has a drift-detection test in `vireo/tests/test_app.py` that fails if it doesn't equal `app.ALL_PAGES` verbatim. Miss any of these and the new default tab is either filtered out server-side, rejected on save, or crashes the drift test.

- *Failing tests:* `DEFAULT_TABS` contains `"import"` immediately before `"pipeline"`; `"import"` is in `ALL_NAV_IDS` and `Database.set_tabs(["import", ...])` succeeds; `app.ALL_PAGES` contains an `import` entry (id/label/href) and the `_navbar.html` `NAV_ALL_PAGES` block still matches it (existing drift test); a fresh DB's workspace gets it; existing workspaces get `import` inserted by the same reset-in-place pattern as the 2026-04-30 unified-tabs migration (solo-user, no preservation shim); navbar renders Import and Process labels.
- *Commit:* `feat: Import tab + Process relabel`

## Task 3.5: User-first scenario (Playwright)

**Files:**
- Test: extend `vireo/tests/test_userfirst_scenarios.py` / `vireo/testing/userfirst/` harness

Drive a real browser like a user (per the user-first testing convention): open Import, pick a fixture card and temp archive destination, choose "After import: Quick look", start, watch per-folder progress appear, see the completion state list the new folders and the safe-to-format pill, confirm the chained process job appears on the Jobs panel, then re-run the same import and confirm everything reports duplicate/no-op (nothing re-copies). If the harness can't cover a step (e.g. SSE timing), assert the API-level equivalent and leave a comment saying which pixels were not verified — don't fake it.

*Commit:* `test: user-first import→process scenario`

## Task 3.6: Full suite + PR

Run the test command plus the userfirst scenario file. `gh pr create --base main` titled "Import page, Process page, and after-import chaining (import/process split PR 3)", referencing #1101/#1102/#1103/#1107. Push review fixes to the same branch.

---

## Execution notes

- After this PR, the split is user-complete: import from the Import page, process from the Process page, chaining via the menu. PR 4 (demolition of staging, `_deindex_staging`, batching, the wizard's import mode backend) follows, plus the separately-deferred remote-destination import (Task 2.7 of the PR 2 plan) — neither blocks the other.
- The wizard's import mode is removed from the UI here but its backend stays until PR 4 — no flag-day.
