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
  imports it from there.
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
- Repositories (`vireo/repositories/`) own SQL for one domain; `Database`
  is the façade over them. Simple persistence operations are reached through
  a domain accessor, a property that builds a fresh repository through the
  domain's `_<domain>_repository` factory on every access
  (`db.job_history.get(job_id)`), so workspace scoping and connection policy
  stay in one place without a forwarding alias per query. `Database` keeps the
  coordinated workflows (`add_photo` and the like), cross-domain composition
  and the active-workspace state. Older domains still carry one-line
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
- Schema changes are ordered migrations in `vireo/schema.py`. They execute once
  at startup, use a transaction, advance `PRAGMA user_version`, and validate
  before committing. Request connections must use the initialized schema. The
  schema they build on (`CREATE TABLE IF NOT EXISTS` for the shape at
  `BASELINE_VERSION`) lives in `vireo/canonical_schema.py`. A column is added
  by a migration, not by an inline `ALTER` there.
- Shared browser code is exposed through the `Vireo` namespace. Network calls
  use `Vireo.api`; shared DOM state uses `Vireo.dom`. New inline event handlers
  and page-global variables are not permitted.

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

ESLint checks first-party JavaScript under `vireo/static`, excluding vendor and
minified files. Classic scripts still share page globals and expose inline-handler
functions, so `no-undef` and `no-unused-vars` are enabled only for the three Browse
controllers listed in `eslint.config.mjs`. Other recommended correctness rules
apply across the static scripts; intentionally empty catch blocks are allowed.
Inline template scripts continue to use the existing rendered-script parsing and
browser tests.

TypeScript checks those controllers' JavaScript with `checkJs`, strict types,
DOM types, and no output files. `types/frontend.d.ts` describes their public state,
selection data, and legacy action dependencies. It checks implementation and
call contracts; it does not validate server responses at runtime or check the
implementation of legacy actions outside the selected files.

When encapsulating another controller, add it to the strict ESLint list and
`tsconfig.frontend.json`, describe its dependencies with narrow JSDoc or declaration
types, and add meaningful negative cases to `tests/frontend/type-contracts.ts`.
Avoid `any` or blanket suppression to make a controller pass. The expected type
errors and ESLint regression tests ensure the checks continue rejecting invalid
code as configuration changes.

### Performance budgets

The large-library benchmark enforces these 100,000-photo pull-request budgets:

- Application startup: 5 seconds
- Browse initialization: 2 seconds
- Folder tree: 1 second
- Job polling: 0.5 seconds

A weekly one-million-photo run uses relaxed budgets of 15, 5, 2, and 1 second
respectively. Schema migration, filesystem walks, and model loading must not
occur on request connections.
