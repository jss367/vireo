# Vireo Architecture and Change Boundaries

Vireo is a local desktop application with a Flask and SQLite core, a
server-rendered vanilla JavaScript interface, and a Tauri native shell. The
filesystem and XMP sidecars remain the durable source of truth; SQLite indexes
that information for interactive use.

## Application layout

- `vireo/app.py` holds the app factory and its wiring, plus the process
  entry point (`main()`), the startup cache migrations, and the mount-aware
  Trash helpers (`_trash_paths` and friends, which tests patch on the `app`
  module). `create_app` builds the per-app objects (the request DB getter,
  the job runner, and the services that carry state), runs the startup
  catalog repairs (`StartupTasks.run_catalog_repairs`), registers the request hooks (`web.app_hooks`), registers
  every blueprint, and adds the `/api/v1` aliases. It defines no routes:
  `test_no_new_routes_in_app_py` holds the limit at zero.
- Blueprints live in `vireo/web/`, one module per route group, each built by
  `create_<domain>_blueprint(get_db, json_error, ...)`. A factory takes as
  arguments only what is per-app: the request DB getter, `json_error`, the
  job-runner getter, `db_path` / `app.config`, and methods of per-app service
  instances. `create_app` passes the bound method a blueprint calls (for
  example `run_batch_delete=photo_deletion.run_batch_delete`), not the whole
  service, except for objects a blueprint uses as a unit (`VisualScope`,
  `LocationErrors`, `InatTokenGeneration`). Anything without per-app state
  (pure helpers, constants, module-level locks) is imported by the blueprint
  module directly, never threaded through `create_app`.
- Shared helpers, by kind: response shapes in `web.responses` (`json_error`,
  `photo_not_found_error`); request parsers, scope guards and page/selection
  caps in `web.request_args`; the settings-file raw reader and write lock in
  `config` (`read_raw_config_file`, `settings_write_lock`, process-global
  because `config.json` is); SQLite IN-clause chunking in `sql_chunks`;
  page payload builders in `highlights_payload` and `best_batch`.

## Dependency boundaries

- HTTP blueprints validate requests and serialize responses. New route groups
  belong under `vireo/web`; do not add routes to `vireo/app.py`.
- Services (`vireo/services/`) own filesystem work, subprocesses, cache
  invalidation, and workflow coordination. A route should call a service
  rather than implement those operations itself. Services never import from
  `vireo/web/`; a definition both layers need (job-type constants, value
  parsers) lives in the service or another neutral module and the web layer
  imports it from there. Every service catalog query and write goes through
  a domain API; no service touches `Database.conn`. Services retain their
  filesystem coordination and public `Database` transaction boundaries.
- Routes that launch a background job use `@background_job` from
  `vireo/web/background_jobs.py`. The view receives a `JobLaunch` (runner,
  active workspace id, worker-thread database factory) and returns
  `ctx.start(job_type, work, ...)`; do not re-implement that prologue inline.
- Processing stages live in `vireo/pipeline_stages/`: scanning and collection
  creation, thumbnails and previews, model loading, detection, classification,
  masks and eye keypoints, and grouping and miss detection. `pipeline_job.py`
  owns thread scheduling, pause participation, the regroup/misses lock, and
  final failure aggregation. Stages receive a fresh `PipelineRun` and explicit
  queues, events, model/detection outputs, and helper callbacks; they never
  import the orchestrator. `PipelineControl` distinguishes parking checkpoints
  from cancellation probes that are safe while holding a lock. The collection
  stage publishes `run.collection_id` before signalling `collection_ready`;
  later stages read that shared value instead of capturing an earlier ID.
  Existing helper entry points remain in `pipeline_job.py` for callers and
  diagnostics, and `PipelineParams` remains importable from there. New stage
  work belongs in the stage modules, not inside `run_pipeline_job`.
- The standalone classify job (`/api/jobs/classify`) is the
  `vireo/classify_job/` package, one module per responsibility: label loading,
  the classifier_runs gate, cache coverage, label-source naming, detection,
  the classify pass, prediction storage, the cached-only finalize, and the
  `run_classify_job` entry point with `_ClassifyJobRun`. `__init__.py`
  re-exports what other modules import (the pipeline stages reuse its
  detection, batching and storage helpers through the package). Tests patch
  a name in the submodule that looks it up, because a patch of the package
  attribute reaches only callers that read it through the package.
- The import job is the `vireo/import_job/` package, one module per
  responsibility (planning, the batch loop, preflight, the duplicate gate,
  transports, copy-and-verify, cataloging, resume, the result). Its
  `__init__.py` re-exports only what callers outside the package import
  (`run_import_job`, `ImportParams`, `copy_and_hash_verify`,
  `DestReadCancelled`, and the snapshot and fingerprint helpers the import
  services use). Its catalog SQL goes through `db.imports`
  (`repositories/imports.py`). The modules import shared helpers by name, so
  a test patches a helper in the module that looks it up, or with
  `vireo/tests/import_job_patching.patch_import_job` when several do.
- Repositories (`vireo/repositories/`) own SQL for one domain; `Database`
  is the façade over them. Simple persistence operations are reached through
  a domain accessor, a property that builds a fresh repository through the
  domain's `_<domain>_repository` factory on every access
  (`db.job_history.get(job_id)`), so workspace scoping and connection policy
  stay in one place without a forwarding alias per query. Coordinated
  workflows (`photo_ingest.add_photo` and the like) and cross-domain
  composition live in `vireo/services/` as functions taking `db`; `Database`
  keeps the active-workspace state, the process caches and the methods a
  contract or a repository binds by name. Older domains still carry one-line
  forwarding wrappers; `test_db_facade_structure.py` caps their number at
  `FORWARDING_WRAPPER_LIMIT`, which only shrinks as domains move to
  accessors. `Database` runs no SQL itself: `test_db_facade_structure.py` fails if a
  `Database` method other than the connection-lifecycle ones uses
  `self.conn` for anything but handing it to a repository.
  Code outside the data layer goes through `Database` methods too:
  `test_sql_boundary.py` forbids its private workspace state
  (`_active_workspace_id`, `_ws_id()`) outside `db.py` and the repositories,
  and caps each module's remaining `<expr>.conn` uses at today's count, so
  that SQL can only move into repositories, never grow.
  Transaction control goes through `Database` as well: `db.commit()`,
  `db.rollback()`, `db.in_transaction`, `db.begin()` (a deferred `BEGIN`,
  the read snapshot multi-query reads hold), `db.begin_immediate()` and
  `with db.transaction():` (sqlite3's `with conn:`) are the connection calls
  they name (`commit()` still honors `_commits_held`), and
  `db.commit_with_retry()` commits with the locked/busy backoff of the
  module's `commit_with_retry`, so a caller that owns a transaction never
  needs `db.conn` for it. `db.set_progress_handler()` is the connection's
  progress-handler call, which installs the search-lane interrupt.
- Workspace-local copy persistence lives in `repositories/local_workspaces.py`;
  folder-local copies and their shared catalog path updates live in
  `repositories/local_folders.py`. These local-copy writes never commit,
  so services keep staging, activation and restoration atomic.
- Schema changes are ordered migrations in `vireo/schema.py`. They execute once
  at startup, use a transaction, advance `PRAGMA user_version`, and validate
  before committing, after a `.pre-v<N>.bak` snapshot of the catalog. Request
  connections must use the initialized schema. The schema they build on
  (`CREATE TABLE IF NOT EXISTS` for the current shape) lives in
  `vireo/canonical_schema.py`. A column is added by a migration, not by an
  inline `ALTER` there, and no module outside the schema modules creates
  tables, indexes, triggers or views on first write:
  `test_schema_ddl_boundary.py` caps such DDL at its current count.
- Shared browser code is exposed through the `Vireo` namespace. Network calls
  use `Vireo.api`; shared DOM state uses `Vireo.dom`; `escapeHtml`,
  `escapeAttr` and `formatBytes` come from `vireo/static/vireo-utils.js`,
  which the navbar loads before any other script, and have no page-local
  copies (`test_shared_frontend_helpers.py`). New inline event handlers and
  page-global variables are not permitted. The dates for retiring the
  remaining per-page helper copies and inline template scripts are in
  `docs/plans/2026-10-16-frontend-schema-consolidation-plan.md`.

## Compatibility rules

- `/api/v1` is the stable automation API and retains token authentication.
- Internal browser endpoints may evolve with the bundled interface but retain
  route and response compatibility during domain extraction.
- Direct workspace tabs remain the primary navigation model. Existing page
  links and user-selected tab sets remain valid as workflows evolve.
- Customized workspace tabs are user data. Migrations may update only a known,
  untouched historical default unless a separate user-facing migration exists.

## Required checks

Pull requests run Python tests and linting, critical Playwright journeys, route
and API-response contract checks, and Rust formatting, linting, and unit tests
when applicable. Nightly and release workflows run the complete Playwright
suite before release artifacts are built.

### Frontend checks

With Node.js 24, run these commands from the repository root:

```bash
npm ci
npm run check:frontend
```

Both pull-request and main workflows run this command. The pull-request `test`
gate requires the frontend job to pass. The individual commands are
`lint:frontend`, `typecheck:frontend`, and `test:frontend-checks`.

`test:frontend-checks` also runs `tests/frontend/function-length.test.mjs`,
the JavaScript twin of the Python function-length ratchet: every function in a
first-party script under `vireo/static` is capped at 100 lines, the ones
already over it are listed with their exact length in
`tests/frontend/function_length_baseline.json`, and that list only shrinks.

ESLint checks first-party JavaScript under `vireo/static`, excluding vendor and
minified files. Classic scripts still share page globals and expose inline-handler
functions, so `no-undef` and `no-unused-vars` are enabled only for the encapsulated
controllers listed in `eslint.config.mjs`. Other recommended correctness rules
apply across the static scripts; intentionally empty catch blocks are allowed.
Inline template scripts continue to use the existing rendered-script parsing and
browser tests.

TypeScript checks every first-party JavaScript file under `vireo/static`,
excluding vendor and minified assets, with `checkJs`, strict types,
`noUncheckedIndexedAccess`, DOM types, and no output files. The page projects in
`tsconfig.frontend-pages/` extend `tsconfig.frontend.base.json`; each classic-script
page has its own program so unrelated page globals do not collide.
`tsconfig.frontend.json` also checks the original controller contract tests.
`types/frontend.d.ts` describes Browse state,
selection data, and legacy action dependencies; `types/dialogs.d.ts` describes the
shared dialog lifecycle. `types/panel-loaders.d.ts` covers the panel loader inputs,
selection responses, and renderer dependencies. `types/shared-controllers.d.ts`
covers visibility-aware polling and grouped prediction decisions. The thumbnail
queue declares its image/request state locally and narrows observer nodes before
using image-only properties. Page payloads and external dependencies live in
`types/pages/`, while JSDoc annotates implementation state and inputs. These
projects check actual JavaScript bodies and their call sites. Inline template
scripts retain rendered-script parsing and browser coverage. Runtime
validation is separate: grouped prediction responses enter as `unknown`, only
positive safe-integer ids are credited, and invalid response members cannot
enter the decision tracker.

`scripts/check_frontend_types.mjs` discovers every page project, verifies its
effective strict options, checks that every first-party script is included,
rejects implementation suppressions, then compiles each program. Adding a script
without including it in a strict project fails `npm run typecheck:frontend`.
Run `npx tsc --project tsconfig.frontend-pages/<page>.json` for a focused check.

When adding a controller, include its implementation in the appropriate strict
page project, describe its dependencies with narrow JSDoc or declaration types,
and add meaningful negative cases for public contracts. Enable the strict ESLint
rules as its shared-global dependencies become explicit.
Avoid `any` or blanket suppression to make a controller pass. The expected type
errors and ESLint regression tests ensure the checks continue rejecting invalid
code as configuration changes. A classic script whose top-level functions are
called from other scripts names them in an `/* exported ... */` directive so
`no-unused-vars` knows they are the file's API, and the navbar globals a strict
file calls (`showToast`, `safeEventSource`, ...) are declared in
`types/navbar.d.ts` and listed as that file's ESLint `globals`.

### Performance budgets

The large-library benchmark enforces these 100,000-photo pull-request budgets:

- Application startup: 5 seconds
- Browse initialization: 2 seconds
- Folder tree: 1 second
- Job polling: 0.5 seconds

A weekly one-million-photo run uses relaxed budgets of 15, 5, 2, and 1 second
respectively. Schema migration, filesystem walks, and model loading must not
occur on request connections.
