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
  catalog repairs, registers the request hooks (`web.app_hooks`), registers
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
- Repositories own SQL for one domain. `Database` remains a compatibility
  façade while photo, workspace, metadata, and job access is extracted.
  Each moved method stays on `Database` as a one-line wrapper around its
  repository, and a structural test keeps the moved methods off `self.conn`.
- Schema changes are ordered migrations in `vireo/schema.py`. They execute once
  at startup, use a transaction, advance `PRAGMA user_version`, and validate
  before committing. Request connections must use the initialized schema.
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

The large-library benchmark enforces these 100,000-photo pull-request budgets:

- Application startup: 5 seconds
- Browse initialization: 2 seconds
- Folder tree: 1 second
- Job polling: 0.5 seconds

A weekly one-million-photo run uses relaxed budgets of 15, 5, 2, and 1 second
respectively. Schema migration, filesystem walks, and model loading must not
occur on request connections.
