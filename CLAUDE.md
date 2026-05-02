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

## Architecture

- `vireo/app.py` — Flask app with all routes. Created via `create_app(db_path, thumb_cache_dir)`.
- `vireo/db.py` — `Database` class. SQLite with workspace support. Auto-creates Default workspace and restores last-used workspace on init.
- `vireo/duplicates.py` — Pure exact-duplicate resolver (winner/loser decision + metadata merge). Consumed by `db.apply_duplicate_resolution` and the scan job.
- `vireo/jobs.py` — `JobRunner` for background tasks (scan, classify, thumbnails, etc.) with SSE progress streaming.
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
3. Run tests before finishing: `python -m pytest tests/test_workspaces.py vireo/tests/test_db.py vireo/tests/test_app.py vireo/tests/test_photos_api.py vireo/tests/test_edits_api.py vireo/tests/test_jobs_api.py vireo/tests/test_darktable_api.py vireo/tests/test_config.py -v`
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
