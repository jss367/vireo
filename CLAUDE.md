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

Layering: `vireo/app.py` wires the app; HTTP routes live in blueprints under `vireo/web/`; workflow, filesystem and cache logic lives in `vireo/services/` (which never imports from `vireo/web/`); SQL lives in `vireo/db.py` and `vireo/repositories/`. See `docs/ARCHITECTURE.md` for the boundaries.

**Injection convention.** Every blueprint is built by `create_<domain>_blueprint(get_db, json_error, ...)`. A factory takes only per-app state: the request DB getter, `json_error`, the job-runner getter (`lambda: app._job_runner`), `db_path` / `app.config`, and methods of per-app service instances, which `create_app` passes as bound methods at registration (e.g. `run_batch_delete=photo_deletion.run_batch_delete`). Objects a blueprint uses as a unit (`VisualScope`, `LocationErrors`, `InatTokenGeneration`) are passed whole. Pure helpers, constants and module-level locks are imported by the blueprint module, never threaded through `create_app`.

- `vireo/app.py` — App factory and wiring, created via `create_app(db_path, thumb_cache_dir, api_token)`, plus `main()`, the startup cache migrations/quota passes, and the mount-aware Trash helpers (`_trash_paths`, `_network_volume_roots`, `_path_on_network_volume`, ...; tests patch these on the `app` module, so blueprints and `PhotoDeletion` receive late-binding lambdas around them). `create_app` builds the per-app objects (`_get_db`, the `JobRunner`, `RenderCache`, `PhotoDeletion`, `StartupTasks`, `MissingOriginals`, `VisualScope`, `BulkGpsLocations`, `FolderMoves`, `PipelineChain`, `build_scan_work`), runs the startup catalog repairs and config migrations, registers the request hooks and every blueprint, and adds the `/api/v1` aliases. It defines no routes: `test_no_new_routes_in_app_py` holds the limit at zero. Missing-originals invalidation is captured by `FolderMoves`, `build_scan_work` and `PipelineChain` at construction, so `missing_originals` is built before them.
- `vireo/db.py` — `Database` class. SQLite with workspace support. Auto-creates Default workspace and restores last-used workspace on init.
- `vireo/repositories/` — Per-domain SQL behind the `Database` façade: `workspaces.WorkspaceRepository` (workspace rows, config-override rewrites, label-set selection, navigation tabs, new-images snapshots), `photo_labels.PhotoLabelRepository` (color labels) and `photo_review.PhotoReviewRepository` (ratings, flags). `Database` keeps every public method as a thin wrapper that builds the repository on `self.conn` (`_workspace_repository(scoped=...)`, `_photo_label_repository()`, ...) and passes the active workspace id explicitly; the active-workspace state, `_ws_id()` and the new-images cache stay on `Database`. `repositories.UNSET` is the shared "argument not provided" sentinel (`db._UNSET`). Structural tests (`test_workspace_method_delegates_to_repository` in `vireo/tests/test_db_workspaces.py`) fail if a moved method touches `self.conn` again.
- `vireo/duplicates.py` — Pure exact-duplicate resolver (winner/loser decision + metadata merge). Consumed by `db.apply_duplicate_resolution` and the scan job.
- `vireo/import_dedup.py` — Metadata-first duplicate gate for imports (`CatalogIndex` + `DuplicateChecker`): match by (filename, size, EXIF capture time) with a content-hash fallback for missing/placeholder metadata; `verify_by_hash` restores hash-everything. Shared by `ingest()`, `/api/import/check-duplicates`, and the local-processing preflight so duplicate previews always agree with what ingest actually skips.
- `vireo/jobs.py` — `JobRunner` for background tasks (scan, classify, thumbnails, etc.) with SSE progress streaming.
- `vireo/config.py` — Global config read/write from `~/.vireo/config.json`, the one-time config migrations, and the settings-write plumbing every route that rewrites the file shares: `read_raw_config_file()` (the on-disk dict without DEFAULTS merged, preserving a `.corrupt` backup) and `settings_write_lock` (module-level, because `config.json` is process-global).
- `vireo/sql_chunks.py` — `chunked(seq, size=SQL_PARAM_CHUNK)`, the IN-clause chunker for caller-sized id lists (re-exported as `app._chunked`; tests patch it there).

### Shared request plumbing (`vireo/web/`)

- `vireo/web/app_hooks.py`, `vireo/web/responses.py` — App-wide request plumbing. `web.responses`: `json_error` (the one JSON error shape, stamped with `g.request_id`) and `photo_not_found_error`. `web.app_hooks`: `get_request_db(db_path)` / `close_request_db` (the per-request `Database` on `g`), `RESERVATION_EXEMPT_ENDPOINTS`, and `register_app_hooks(app, get_db=..., reservation_exempt_endpoints=...)`, which installs request timing/ids, the browser-surface guard, the `/api/v1` token check, the workspace mutation reservation and its release, request logging with the security headers and session cookie, and the uncaught-error handler — `before_request` order is part of the security contract (`test_app_hooks_run_in_security_order`). `create_app`'s `/api/v1` alias loop adds `v1_<view>` to the exempt set it passed in.
- `vireo/web/request_args.py` — The `flask.request` parsers (`request_bool_arg`, `request_rules_arg`, `request_visual_arg`, `request_flag_filter`, `request_location_status_filter`, `dashboard_scope_args`, `request_missing_originals_folder_id`), `focus_candidate_ids`, the `MAX_SELECTION_PHOTOS` / `MAX_FOCUS_PHOTO_IDS` / `MAX_PER_PAGE` caps, and the guards that answer with a response (`reject_visual_collection`, `parse_selection_photo_ids`), which take `json_error` as a keyword. It re-exports `coerce_collection_id` from `services.visual_scope`.
- `vireo/web/background_jobs.py` — `@background_job` decorator + `JobLaunch` context. Every route that starts a job takes `ctx` as its first argument, opens worker DBs with `ctx.thread_db()`, and returns `ctx.start(job_type, work, ...)`.
- `vireo/web/location_edits.py` — Location-edit helpers shared by the location, place, batch keyword/location, and location-review routes: request parsing (`extract_place_id`, `extract_keyword_id`, `normalize_client_place_details`), location payloads (`serialize_photo_location`, `walk_parent_chain`, `location_name_conflict_payload`/`_response`, `LOCATION_NAME_PIPE_ERROR`), and `LocationErrors` — the error responses bound to `json_error`, built once in `create_app` as `location_errors`. Keyword and GPS sidecar queueing with add/remove cancellation lives in `vireo/services/pending_changes.py`; Google result-language handling (`result_language`, `place_details_for_language`, `reverse_geocode_for_language`) in `vireo/places.py`.

### Blueprints (`vireo/web/`)

- `vireo/web/pages.py` — HTML pages: the plain template routes, `/` and `/welcome` (redirect on classification readiness / `setup_complete`), `/config-defaults.js` (the `window.VIREO_*` globals `_navbar.html` loads first), and `/favicon.ico`.
- `vireo/web/pipeline.py` — Pipeline domain: `/api/jobs/pipeline`, every `/api/pipeline/*` endpoint (plan, slots, config, page-init, results, reflow, regroup-live, detach, group state/apply, mask variant), `/api/processes` CRUD, and `/api/photos/<id>/pipeline`. Photo-dict enrichers (`attach_edit_recipes`, `attach_species_representatives`, ...) live in `vireo/photo_payload.py`; cached-results editing (`auto_detach_burst_for_species`, `compute_time_range`, ...) in `vireo/pipeline_results.py`; CPU-only runtime warnings in `vireo/runtime_warnings.py`.
- `vireo/web/imports.py` — Import domain: `/api/import/*` previews, readiness, orphaned-staging, and the `import-full` / `import` / `import-in-place` / `import-photos` jobs. Receives missing-originals invalidation, `PipelineChain`'s `enqueue_process_job` / `chain_after_move`, `BulkGpsLocations.payload`, the `FolderMoves` guard and `app._sync_job_lock`.
- `vireo/web/jobs.py` — Job-control routes (`/api/jobs`, status, cancel, pause, resume, stream, history) and the self-contained launchers (thumbnails, duplicate scan, verify-hashes, model/taxonomy downloads, capture-time, sharpness, cull, regroup).
- `vireo/web/job_launchers.py` — The remaining `/api/jobs/*` launchers: scan, scan-workspace, repair-metadata, previews, ingest, move-photos, move-folder, offline-cache, prepare-full-resolution, sync, classify, develop, extract-masks, fetch-labels, precompute-embeddings, batch-delete. Receives `build_scan_work`, `run_batch_delete`, missing-originals invalidation, the `FolderMoves` methods, the `/photos/<id>/original` view (late-bound) and `app._sync_job_lock`.
- `vireo/web/local_workspace.py`, `vireo/web/local_folder.py` — The `/api/workspaces/active/...` Work Locally staging routes (workspace- and folder-scoped); the logic and the `LOCAL_*_JOB_TYPES` constants live in `vireo/services/local_workspace.py` / `local_folder.py`.
- `vireo/web/settings.py` — Settings domain: `/api/config` (curated form snapshot) and every `/api/settings/*` endpoint (schema, values, global/workspace PATCH and DELETE, export, import), with the working-copy quota confirmation gate and post-save side effects. Also exports `workspace_effective_setting`, the per-workspace override lookup shared with the workspaces and keywords blueprints. Receives `advance_inat_token_generation` (the bound `advance` of the app's one `InatTokenGeneration`, called only while holding `config.settings_write_lock`).
- `vireo/web/inat.py` — iNaturalist domain: every `/api/inat/*` endpoint (prepare, validate-token, token, export, submit, submit-batch, submissions). Owns `InatTokenGeneration` (`current` + `advance()`), the per-app counter shared with the settings blueprint so a settings write that changes `inat_token` supersedes any in-flight token validation; it is only read or advanced while holding `config.settings_write_lock`.
- `vireo/web/card_cleanup.py`, `vireo/web/models.py`, `vireo/web/export.py` — Card-cleanup scan/verify/delete (own job lock); models, darktable status/download, taxonomy info, species-label endpoints, `/api/classify/readiness` and `/api/classify/config`, the MegaDetector download/delete, and the label-embedding cache (`/api/embedding-cache`, `/api/embedding-matrix`), with the startup DB's `count_keywords` injected for the taxonomy count (`classification_readiness(db)` lives in `vireo/classification_readiness.py`); export presets, the export job, and website publishing (which renders the `highlights_payload` builders and receives `VisualScope.resolve`).
- `vireo/web/caches.py` — Derived-data caches and results: `/api/preview-cache`, `/api/computation-cache` (status, export, import), `/api/detection-cache/stats`, `/api/detections/<id>`, and the cached culling analysis (`/api/culling/results`, `/api/culling/apply`).
- `vireo/web/media.py` — Image and mask serving: `/thumbnails/<filename>`, every `/photos/<id>/*` render (`crop`, `full`, `preview`, `edit-mask-preview`, `edit-preview`, `original`), `/masks/<filename>`, `/api/masks/<pid>/<variant>.png`, and `/api/photos/<pid>/masks`, each gated to the active workspace, with the render/full-resolution cache helpers only these routes use. Receives `RenderCache.invalid_preview_cache_paths` / `clear_preview_cache_invalid`; `create_app` binds `serve_original_photo` to the registered `media.serve_original_photo` view for the prepare-full-resolution job. Tests that patch `working_copy_publication_guard`, `touch_working_copy_access` or `_recipe_render_source` for these routes patch `web.media`.
- `vireo/web/audit.py` — Every `/api/audit/*` endpoint (drift, orphans, untracked, stray sidecars, integrity, summary, and the repairs). Receives the `PhotoDeletion` cached-file cleanup and missing-originals invalidation.
- `vireo/web/storage.py` — `/api/storage` (per-location disk-usage report) and every `/api/storage/*` endpoint (masks, files, clear, clear-safe, open-folder, delete-files).
- `vireo/web/workspaces.py` — Every `/api/workspaces/*` endpoint owned by the core app (CRUD, activate, pin, folders, move-folders, active config, subject types, new-images probe/recheck/snapshot) plus `/api/workspace/tabs/*` and `/api/workspace/classification-inventory`. Receives the runner getter, missing-originals invalidation and `app._new_images_walk_progress`.
- `vireo/web/folders.py` — Every `/api/folders*` endpoint (tree, missing, check-health, detail, per-folder workspaces, relocate, delete, rescan job, bulk reveal). Receives `build_scan_work`, the `PhotoDeletion` cached-file cleanup and missing-originals invalidation.
- `vireo/web/remote_setup.py` — NAS setup wizard and remote targets: every `/api/remote-setup/*` endpoint (all loopback-only), `/api/remote-targets` and `/api/remote-targets/test`; `REMOTE_TARGET_PROBE_BUDGET_SECS` is read from `app.config` at request time.
- `vireo/web/editing.py` — Photo-editor endpoints not tied to one photo: `/api/edit-fields`, edit presets, and `/api/editor/crop-ratio`.
- `vireo/web/species.py`, `vireo/web/capture_time.py` — `/api/species` and `/api/species/search`; `/api/capture-time/preview` (the correction math lives in the top-level `capture_time` module).
- `vireo/web/misses.py` — Every `/api/misses/*` endpoint (list, config, threshold preview, recompute, bulk reject, per-photo unflag), scoped by the shared filter bar through `VisualScope.resolve`.
- `vireo/web/life_list.py`, `vireo/web/highlights.py` — Life List: every `/api/life-list/*` endpoint plus the taxonomy explorer builders. Highlights and photo preferences: `/api/highlights*`, `/api/species-highlights*` and `/api/photo-preferences`; confirm and relabel are prediction-decision routes. Both import their payload builders from `vireo/highlights_payload.py`.
- `vireo/web/sync.py` — Every `/api/sync/*` endpoint (queue status, location-write backfill, the progressive XMP before/after preview, discard).
- `vireo/web/duplicates.py` — Every `/api/duplicates/*` endpoint owned by the core app (apply, bulk-resolve, delete-loser-files, last-scan, disk-cleanup-summary). Receives the late-bound Trash helpers from `app.py`, the `PhotoDeletion` cached-file cleanup and missing-originals invalidation.
- `vireo/web/moves.py`, `vireo/web/move_cleanup.py` — Move rules (`/api/move-rules*`) and `/api/move-folder/preflight`; post-move source cleanup.
- `vireo/web/history.py` — `/api/undo`, `/api/redo` (each with a `/status` probe) and `/api/edit-history`; undo and redo are prediction-decision routes. Receives `RenderCache.invalidate_photo_render_cache`.
- `vireo/web/photos.py`, `vireo/web/photo_edit_recipes.py`, `vireo/web/photo_location_keywords.py`, `vireo/web/photo_labels.py`, `vireo/web/photo_review.py` — The `/api/photos` data API, split by concern. `web.photos`: listing and paging, photo detail (aliased as `/api/v1/photos[/<id>]`, so it is registered before the alias loop), text and similarity search, best-batch, region sharpness, subjects, the wildlife-exclusion toggle, open-external, supported extensions, and the Missing Originals routes; receives `visual_scope`, the `MissingOriginals` methods and `run_batch_delete`. `web.photo_edit_recipes`: per-photo edit-recipe get/set/clear/compose, the local-mask snapshot, bulk apply and summary, and edit-recipe history; receives `RenderCache.invalidate_photo_render_cache`. `web.photo_location_keywords`: per-photo keyword add/remove and location edits; receives `location_errors`. `web.photo_labels`: color labels and their descriptions. `web.photo_review`: per-photo and batch rating and flag.
- `vireo/web/browse.py`, `vireo/web/collections.py`, `vireo/web/dashboard.py` — Browse: `/api/browse/init`, `/api/browse/summary`, the filter bar's `/api/filters/*`, the selection panel's `/api/selection/*`, and the folder picker's filesystem routes; receives `visual_scope`. Collections: every `/api/collections*` endpoint (two aliased under `/api/v1/collections`). Dashboard: `/api/dashboard/options`, `/api/stats`, `/api/coverage`.
- `vireo/web/keywords.py` — Every `/api/keywords*` endpoint except link-place. `/api/v1/keywords` aliases `keywords.api_keywords`.
- `vireo/web/batch.py` — Every `/api/batch/*` endpoint over a photo selection. Receives `location_errors`, `BulkGpsLocations.payload`, `run_batch_delete` and missing-originals invalidation.
- `vireo/web/locations.py` — `/api/places/reverse-geocode`, `/api/keywords/<id>/link-place`, and every `/api/location-review/*` endpoint. Receives `location_errors` and the `BulkGpsLocations` selection parsers; the reverse-geocode cache codec comes from `services.gps_locations`.
- `vireo/web/system.py` — `/api/health`, `/api/version` (and `/api/v1` twins), `/api/shutdown` and `/api/v1/shutdown` (exempt from the workspace mutation reservation as `system.api_shutdown` / `system.api_v1_shutdown`), setup, system info, exiftool, scan status, logs, report-issue, volumes, recent destinations, and file reveal.
- `vireo/web/predictions.py`, `vireo/web/encounters.py` — Prediction review. `web.predictions`: every `/api/predictions*` endpoint — the listing, Compare, burst group read, and the decision routes (`<id>/accept`, `accept-subject`, `reject`, `reviewed`, `replace-keywords`, `batch-accept`, `batch-reject`, `group/apply`); receives `visual_scope`. `web.encounters`: `/api/encounters/species`, which rewrites the pipeline results cache next to `db_path`. Both call the decision lock through `services.prediction_decisions`.

### Services and shared logic

- `vireo/services/visual_scope.py` — Visual-search clauses and collection scoping with no Flask dependency: `validate_visual_arg`, `inject_active_visual_model`, `coerce_collection_id`, `collection_row`, `collection_rules_state`, `VISUAL_COLLECTION_MSG`, and `VisualScope` (`resolve` / `apply_to_rules`), which owns the per-app query-text embedding cache.
- `vireo/services/gps_locations.py` — Bulk location assignment from EXIF GPS: `BulkGpsLocations` (built once around `json_error` and `location_errors`; `normalize_photo_id_list`, `source_ids`, `payload`), `location_keyword_photo_ids`, `resolve_exif_place_for_photo`, the language-tagged reverse-geocode cache codec, and `summarize_details`.
- `vireo/services/scan_work.py`, `vireo/services/folder_moves.py` — `scan_work.build_scan_work(...)` builds the worker for every scan job (bound to the app with `functools.partial` in `create_app`); `FolderMoves` owns the move-folder job (`start_job`), the chained enqueue (`enqueue_job`), the local-copy guard (`guard_error`) and `pending_local_workspace_transition`. Tests that swap `_invalidate_new_images_after_scan` for the scan job patch `services.scan_work`.
- `vireo/services/pipeline_launch.py` — `PipelineChain` (the after-import chain: `enqueue_process_job`, `chain_after_move`), `apply_no_model_auto_skip` and `resolve_remote_archive_target`.
- `vireo/services/prediction_decisions.py` — The prediction-decision writer lock shared by every route that records a review decision: `begin_prediction_decision(db, json_error=...)`, `under_prediction_decision_lock(db, work, json_error=...)`, `out_of_workspace_prediction_ids(db, pred_ids)`, and `PREDICTION_DECISION_ROUTES` (view function names, so an entry survives a move between modules). Routes call these through the module (`from services import prediction_decisions`), never under an injected alias. `test_every_prediction_decision_route_locks` derives the decision routes from the call graph of `app.py`, `vireo/web/*.py` and `vireo/services/*.py` and fails until a new one is listed and reaches `begin_prediction_decision`.
- `vireo/services/prediction_ambiguity.py` — Which pending predictions a bare Accept must not act on: `ambiguous_prediction_ids`, `effective_category_resolver`, `prediction_is_ambiguous`. Imported by the browse (selection panel) and predictions (`batch-accept`) blueprints so both split the same way.
- `vireo/services/render_cache.py` — `RenderCache(config)` owns the per-app `invalid_preview_cache_paths` set and provides `invalidate_photo_render_cache`, `clear_preview_cache_invalid` / `mark_preview_cache_invalid`; the stateless `queue_edit_recipe_sync(db, photo_id, recipe_json)` queues a recipe for XMP sync.
- `vireo/services/photo_deletion.py` — `PhotoDeletion.run_batch_delete` (Trash / permanent / catalog-only modes, identity revalidation, pipeline-cache prune, progress phases) and `cleanup_cached_files_for_deleted_photos`. Built once with late-binding wrappers around `app.py`'s `_chunked`, `_trash_paths`, `_snapshot_parent_device` and `_path_confirmed_gone` (tests monkeypatch those on `app`).
- `vireo/services/startup_tasks.py` — `StartupTasks(app, db_path, init_db)` caches the startup taxonomy parse and owns `sync_mark_species_only`, `mark_species`, `retire_wildlife_genre`, `kickoff_thumb_path_backfill` and `cleanup_app_resources`; `create_app` decides when each runs (the `VIREO_DISABLE_STARTUP_BACKFILL_TIMERS` guards) and exposes `app._retire_wildlife_genre`, `app._kickoff_thumb_path_backfill` and `app._cleanup_app_resources`. Also the stateless `metadata_repair_count` (imported by the imports and job-launchers blueprints) and `utc_iso_now()`.
- `vireo/services/missing_originals.py` — `MissingOriginals` owns the per-app scan cache, in-flight table, error/backoff records and invalidation generations (also exposed as `app._missing_originals_{lock,cache,inflight,errors,generation}`), with `payload`, `start_scan`, `invalidate` and `folder_health_loop`. Module-level: `resolve_folder_id`, `cache_key`, `HEAVY_JOB_TYPES`.
- `vireo/highlights_payload.py`, `vireo/best_batch.py` — Pure payload builders (a `Database` in, JSON-ready dicts out): the Highlights and Life List payloads with their bucket scoring, curation-ordering and filter helpers, and `photo_highlight_entries`; `best_batch_scope` / `build_best_batch_response` for `/api/photos/<id>/best-batch`. Imported by the blueprints that render them.
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
