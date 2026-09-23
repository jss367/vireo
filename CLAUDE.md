# Vireo - Claude Code Guide

## What is Vireo

AI-powered wildlife photo organizer. Flask backend, Jinja2 templates, vanilla JS frontend. No frontend framework.

## UI transparency is a hard rule

Read `CORE_PHILOSOPHY.md` ("Show the user what's happening / No black boxes") before writing or reviewing any user-facing status text — pills, badges, counters, summaries, "X of Y", "Already done", "Will run", readiness panels, progress phrases. Each one must answer the question users actually read it as, not a cheaper backend proxy. A pill that says "Already done" must mean *the next run would be a no-op given current settings* — not "there exists prior output somewhere." If the accurate signal needs the current UI selections (selected models, labels, variants, reclassify, etc.), build the endpoint that takes them; don't fall back to a global `COUNT(*) > 0`.

## Running

```bash
python vireo/app.py --db ~/.vireo/vireo.db --port 8080
```

## Key paths

- **App logs**: `~/.vireo/vireo.log` (RotatingFileHandler, 5MB, 3 backups). Check here first when debugging.
- **Database**: `~/.vireo/vireo.db` (SQLite, WAL mode, foreign keys ON)
- **Config**: `~/.vireo/config.json` (global settings — threshold, model, keys)
- **Thumbnails**: `~/.vireo/thumbnails/`
- **Label files**: `~/.vireo/labels/`

## Tests

```bash
# All tests (from repo root)
python -m pytest tests/ vireo/tests/ -q

# Workspace tests only
python -m pytest tests/test_workspaces.py -v

# DB tests only
python -m pytest vireo/tests/test_db.py -v
```

Tests use temp databases. `vireo/tests/test_app.py` isolates config via `cfg.CONFIG_PATH = str(tmp_path / "config.json")` to avoid polluting `~/.vireo/config.json`.

### Impact-selected runs

The full unit suite is ~7.5k tests. `scripts/select_tests.py` maps `git diff` onto a per-test coverage map recorded on `main` and runs only the tests that executed the changed functions (whole file for changed test files; templates/static/data resolved through the Python lines that reference them). Module-level, structurally ambiguous, and harness changes fall back to the full suite because import-time dependencies cannot be represented safely by line coverage.

```bash
python scripts/select_tests.py fetch-map            # newest map from the "Full tests" workflow (needs gh)
python scripts/select_tests.py --run -- -n auto -q   # run the selection
python scripts/select_tests.py --explain             # just print what would run and why
```

CI does the same: PRs (`test.yml`) run the selected subset on Linux; every push to `main` (`test-main.yml`) runs the complete suite on Linux/macOS/Windows, enforces the coverage threshold, and publishes a fresh map to the Actions cache and as the `test-impact-map` artifact. The `ci-full-suite` PR label forces the full suite on a PR.

Do not monkeypatch `sqlite3.connect` globally in tests: coverage flushes per-test contexts to its own SQLite file at every test boundary, so a global fake crashes the xdist worker. Fake only the connection for the database path under test (see `test_pipeline_queue.py`).

### Browser tests

`tests/e2e` is excluded from the default `addopts`, so the browser suite only runs
when asked for by name. It is timing-sensitive: under machine load a page load can
stall past a locator's 30s timeout and fail a test that has nothing wrong with it.
The release gate (`e2e-full.yml`) absorbs that with reruns — match it locally rather
than chasing a one-off red line.

`pip install -e ".[dev]"` installs the Playwright Python package but not the browser
binary, so a clean checkout needs the one-time download first (add `--with-deps` on
Linux to pull the system libraries too, as `e2e-full.yml` does):

```bash
python -m playwright install chromium
python -m pytest -o addopts='' -q tests/e2e/ --reruns 2 --reruns-delay 1
```

Reruns retry only the failed test, with fresh fixtures. A genuinely broken test still
fails all three attempts; only one that passes on retry is tolerated.

## Architecture

- `vireo/app.py` — Flask app factory and the legacy routes not yet moved to `vireo/web/`. Created via `create_app(db_path, thumb_cache_dir)`. Do not add routes here: `test_no_new_routes_in_app_py` caps the count, and PRs that move routes out lower the cap.
- `vireo/db.py` — `Database` class. SQLite with workspace support. Auto-creates Default workspace and restores last-used workspace on init.
- `vireo/duplicates.py` — Pure exact-duplicate resolver (winner/loser decision + metadata merge). Consumed by `db.apply_duplicate_resolution` and the scan job.
- `vireo/import_dedup.py` — Metadata-first duplicate gate for imports (`CatalogIndex` + `DuplicateChecker`): match by (filename, size, EXIF capture time) with a content-hash fallback for missing/placeholder metadata; `verify_by_hash` restores hash-everything. Shared by `ingest()`, `/api/import/check-duplicates`, and the local-processing preflight so duplicate previews always agree with what ingest actually skips.
- `vireo/jobs.py` — `JobRunner` for background tasks (scan, classify, thumbnails, etc.) with SSE progress streaming.
- `vireo/web/background_jobs.py` — `@background_job` decorator + `JobLaunch` context. Every route that starts a job takes `ctx` as its first argument, opens worker DBs with `ctx.thread_db()`, and returns `ctx.start(job_type, work, ...)`.
- `vireo/web/pipeline.py` — Pipeline domain: `/api/jobs/pipeline`, every `/api/pipeline/*` endpoint (plan, slots, config, page-init, results, reflow, regroup-live, detach, group state/apply, mask variant), `/api/processes` CRUD, and `/api/photos/<id>/pipeline`. Built by `create_pipeline_blueprint(...)`. Photo-dict enrichers (`attach_edit_recipes`, `attach_species_representatives`, ...) live in `vireo/photo_payload.py`; cached-results editing (`auto_detach_burst_for_species`, `compute_time_range`, ...) in `vireo/pipeline_results.py`. The after-import chain (`enqueue_process_job`, `chain_after_move`) lives in `vireo/services/pipeline_launch.py` as `PipelineChain`, alongside `apply_no_model_auto_skip` and `resolve_remote_archive_target`; CPU-only runtime warnings are in `vireo/runtime_warnings.py`.
- `vireo/web/card_cleanup.py`, `vireo/web/models.py`, `vireo/web/export.py` — Card-cleanup scan/verify/delete (own job lock); models, darktable status/download, taxonomy info, and species-label endpoints (`classification_readiness(db)` lives in `vireo/classification_readiness.py`); export presets, the export job, and website publishing. All built by `create_*_blueprint(...)` with settings-file access and page payload builders injected.
- `vireo/web/imports.py` — Import domain: `/api/import/*` previews, readiness, orphaned-staging, and the `import-full` / `import` / `import-in-place` / `import-photos` jobs with their import-only helpers. Built by `create_imports_blueprint(...)`; cross-domain helpers (missing-originals cache, visual-collection guard, process enqueue, after-move chaining, GPS payload) are injected from `create_app`.
- `vireo/web/jobs.py` — Job-control routes (`/api/jobs`, status, cancel, pause, resume, stream, history) and the self-contained launchers (thumbnails, duplicate scan, verify-hashes, model/taxonomy downloads, capture-time, sharpness, cull, regroup). Built by `create_jobs_blueprint(...)`; launchers that need import/pipeline/settings helpers are still in `app.py`.
- `vireo/web/settings.py` — Settings domain: `/api/config` (curated form snapshot) and every `/api/settings/*` endpoint (schema, values, global/workspace PATCH and DELETE, export, import), with the working-copy quota confirmation gate and post-save side effects. Also exports `workspace_effective_setting`, the pure per-workspace override lookup shared with the workspaces blueprint and a keywords route in `app.py`. Built by `create_settings_blueprint(...)`; `settings_write_lock`, `read_raw_config_file` and `advance_inat_token_generation` (the bound `advance` of the app's one shared `web.inat.InatTokenGeneration`, called only while holding `settings_write_lock`) are injected from `create_app`.
- `vireo/web/audit.py` — Audit domain: every `/api/audit/*` endpoint (drift, orphans, untracked, stray sidecars, integrity, summary, and the resolve / accept-hash / remove-orphans / import-untracked / delete-sidecars repairs). Built by `create_audit_blueprint(...)`; scan roots come from the active workspace, and the cached-file cleanup and missing-originals cache invalidation are injected from `create_app`.
- `vireo/web/storage.py` — Storage domain: `/api/storage` (per-location disk-usage report and backing-volume totals for catalog, thumbnails, working copies, offline originals, masks, models, HuggingFace cache) and every `/api/storage/*` endpoint (masks, masks/delete-variant, masks/delete-inactive, masks/delete-stale, files, clear, clear-safe, open-folder, delete-files), with the `_storage_masks_data` and `_clear_storage_cache` helpers only these routes used. Built by `create_storage_blueprint(...)`; `app.config` paths are read at request time and `_chunked` (shared with the batch-delete routes) is injected from `create_app`.
- `vireo/web/inat.py` — iNaturalist domain: every `/api/inat/*` endpoint (prepare, validate-token, token, export, submit, submit-batch, submissions) with the two helpers only they use (`_inat_edit_recipe_source`, `_inat_upload_photo_path`); the export job builds its own `background_job` via `make_background_job`. Owns `InatTokenGeneration` (`current` + `advance()`) — the per-app counter shared with the settings blueprint, whose bound `advance` is injected as `advance_inat_token_generation` so a settings write that changes `inat_token` supersedes any in-flight modal token validation; the counter is only read or advanced while holding `settings_write_lock`. Built by `create_inat_blueprint(...)`; `settings_write_lock`, `read_raw_config_file`, and `max_selection_photos` are injected from `create_app`.
- `vireo/web/workspaces.py` — Workspaces domain: every `/api/workspaces/*` endpoint owned by the core app (CRUD, activate, pin, folders GET/POST/DELETE, move-folders, active config GET/POST, subject types GET/PUT, new-images probe/recheck/snapshot create/get) plus the `/api/workspace/tabs/*` navigation routes. `_validate_workspace_config_overrides` and the `_new_images_walk_*` / `_new_images_deferred_reason` helpers are closures in the factory; `_NEW_IMAGES_SYNC_WAIT_SECS` is a module constant. The `/api/workspaces/active/...` local-workspace and local-folder staging routes have their own blueprints (`web.local_workspace` / `web.local_folder`). Built by `create_workspace_blueprint(...)`; `get_runner`, `invalidate_missing_originals`, `settings_write_lock`, `new_images_walk_progress` (the `app._new_images_walk_progress` dict) and `missing_originals_heavy_job_types` are injected from `create_app`.
- `vireo/config.py` — Global config read/write from `~/.vireo/config.json`.
- `vireo/templates/_navbar.html` — Shared navbar included by all pages. Contains workspace switcher, bottom panel, lightbox, theme system.
- `vireo/templates/*.html` — One file per page, inline CSS and JS.

## Workspaces

Each workspace scopes predictions, collections, pending changes, and visible folders. Photos and keywords are global (shared across workspaces).

- `Database.__init__` auto-creates "Default" workspace and restores the last-used workspace (by `last_opened_at`).
- Workspace-scoped methods use `self._ws_id()` which raises `RuntimeError` if no workspace is active.
- Background job threads must call `thread_db.set_active_workspace(active_ws)` after creating a `Database` instance.
- Per-workspace config overrides are stored in `workspaces.config_overrides` (JSON column). Use `db.get_effective_config(cfg.load())` to get config with workspace overrides applied.

## Workflow

**All feature work, bug fixes, and non-trivial changes MUST be done in a git worktree.** Do not make changes directly on `main`. At the start of any implementation task, create a worktree before writing code.

1. Create a worktree and feature branch for the task.
2. Do all implementation work in the worktree.
3. Run tests before finishing. Preferred: `python scripts/select_tests.py --run -- -n auto -q` (after `fetch-map`), which runs exactly what your diff can affect. Fallback when no map is available: `python -m pytest tests/test_workspaces.py vireo/tests/test_db.py vireo/tests/test_app.py vireo/tests/test_photos_api.py vireo/tests/test_edits_api.py vireo/tests/test_jobs_api.py vireo/tests/test_darktable_api.py vireo/tests/test_config.py -v`
4. **Create a PR** using `gh pr create`. Include what changed and test results in the PR description.
5. When review feedback arrives, push fixes to the **same branch**. The review bot re-reviews automatically on push.
6. Squash-merge when approved.

## Debugging tips

- Slow page navigation? Check if the bottom panel's SSE log stream or job polling is consuming Flask threads. The SSE stream and polling only run when the panel is open.
- Request timing is logged for all API calls at INFO level and slow requests (>0.5s) at WARNING level.
- The Flask dev server is single-process with threading. Long-running SSE connections can exhaust the thread pool.

## PR Agent System

Automated review cycle managed by `.github/workflows/pr-agent.yml`.

### How it works

1. Someone comments `/claude-fix` on a PR to activate the agent.
2. Claude reads review comments and pushes fixes to the **same branch**.
3. When a review is submitted on a `claude-agent` PR (not an approval), Claude pushes fixes to the branch.
4. When **Codex Connect** submits a review on any PR, Claude addresses the feedback by pushing to the branch and adds the `claude-agent` label so future comments are handled automatically.
5. When the **Tests workflow fails** on any PR, Claude reads the failure logs and pushes a fix directly to the PR branch. Loop prevention: skips if the failing commit was already a CI fix attempt.
6. When an **approving review** is submitted or someone comments **👍**, the PR is squash-merged.
7. Branches are deleted after merge.

### Key files

- `.github/workflows/pr-agent.yml` — Event forwarder + pure-bash merge jobs
- `.github/actions/fire-routine/action.yml` — Composite action that POSTs to the routine `/fire` endpoint
- `docs/pr-agent-routine.md` — Setup guide for the Claude Code routine that does the LLM work
- `docs/pr-agent-routine-prompt.md` — The routine's prompt (paste into claude.ai/code/routines)

### Architecture

LLM work runs in a Claude Code routine (billed against the Code subscription, not the Anthropic API). The GHA workflow only classifies events and fires the routine with a text payload. Three merge jobs remain pure bash. See `docs/pr-agent-routine.md`.
