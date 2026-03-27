# Vireo - Claude Code Guide

## What is Vireo

AI-powered wildlife photo organizer. Flask backend, Jinja2 templates, vanilla JS frontend. No frontend framework.

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

## Database schema (key tables)

Global: `folders`, `photos`, `keywords`, `photo_keywords`
Workspace-scoped: `predictions`, `collections`, `pending_changes` (all have `workspace_id` FK with `ON DELETE CASCADE`)
Workspace management: `workspaces`, `workspace_folders`

## Workflow

**All feature work, bug fixes, and non-trivial changes MUST be done in a git worktree.** Do not make changes directly on `main`. At the start of any implementation task, create a worktree before writing code.

1. Create a worktree and feature branch for the task.
2. Do all implementation work in the worktree.
3. Run tests before finishing: `python -m pytest tests/test_workspaces.py vireo/tests/test_db.py vireo/tests/test_app.py vireo/tests/test_photos_api.py vireo/tests/test_edits_api.py vireo/tests/test_jobs_api.py vireo/tests/test_darktable_api.py vireo/tests/test_config.py -v`
4. **Create a PR** using `gh pr create`. Include what was changed and test results in the PR description.

## Debugging tips

- Slow page navigation? Check if the bottom panel's SSE log stream or job polling is consuming Flask threads. The SSE stream and polling only run when the panel is open.
- Request timing is logged for all API calls at INFO level and slow requests (>0.5s) at WARNING level.
- The Flask dev server is single-process with threading. Long-running SSE connections can exhaust the thread pool.

## PR Agent System

Automated review cycle managed by `.github/workflows/pr-agent.yml`.

### How it works

1. Someone comments `/claude-fix` on a PR to activate the agent.
2. Claude reads review comments, creates a **new fix PR** targeting the original PR's branch.
3. The fix PR gets the `claude-agent` label automatically.
4. When a review is submitted on a `claude-agent` PR (not an approval), Claude creates another fix PR.
5. When an **approving review** is submitted or someone comments **👍**, the merge chain starts.
6. The chain squash-merges from leaf to root, running tests between each merge.
7. Branches are deleted after merge.

### PR chain structure

Each fix PR includes `Parent PR: #N` in its body. This is how the merge chain traces the ancestry:

```
main ← feature-v1 (PR A, original)
         ← feature-v1-fix-1 (PR B, fixes round 1)
            ← feature-v1-fix-1-fix-2 (PR C, fixes round 2)
```

### Conflict detection

When `main` receives a push, the workflow checks all open `claude-agent` PRs for merge conflicts and resolves them automatically.

### Key files

- `.github/workflows/pr-agent.yml` — Workflow with all jobs
- `.github/scripts/merge-chain.sh` — Squash-merge chain logic
