# Vireo Architecture and Change Boundaries

Vireo is a local desktop application with a Flask and SQLite core, a
server-rendered vanilla JavaScript interface, and a Tauri native shell. The
filesystem and XMP sidecars remain the durable source of truth; SQLite indexes
that information for interactive use.

## Dependency boundaries

- HTTP blueprints validate requests and serialize responses. New route groups
  belong under `vireo/web`; do not add routes to the legacy application module.
- Services own filesystem work, subprocesses, cache invalidation, and workflow
  coordination. A route should call a service rather than implement those
  operations itself.
- Repositories own SQL for one domain. `Database` remains a compatibility
  façade while photo, workspace, metadata, and job access is extracted.
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
