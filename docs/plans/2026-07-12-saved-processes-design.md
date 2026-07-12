# Saved Processes — design

**Date:** 2026-07-12
**Branch:** `import-pipeline-customization`

## Problem

Today, the "process strategies" a user can pick when importing (`identify`
= "Identify birds", `full`, `cull_ready`, `quick_look`) are **hardcoded** in
`vireo/process_strategies.py` as a Python `STRATEGIES` dict. There is no config
file, DB table, or UI to create, edit, rename, or delete them. The only
customization is the process page's "Custom (use toggles)" mode (a one-off, not
saved) and picking one of the four as a per-workspace default.

A strategy is essentially a **saved snapshot of the process page's stage
toggles** — but two of the six values it carries (`miss_enabled`,
`review_mode`) have no control on the process page and are injected from the
preset server-side. That mismatch ("a preset does more than the toggles show")
is the source of the current confusion.

## Goal

Replace hardcoded strategies with a first-class **"saved process"** concept:
a named, user-managed snapshot of the full process-page state. Users see, add,
edit, and delete them on the **process page** (the editor). The **import page**
and the per-workspace default setting are pure **consumers** — a picker over
the saved-process library.

## Decisions (from brainstorming)

1. **Everything is a saved process.** Ship the four as editable seed rows; one
   is marked the app default. No special read-only built-ins.
2. **Surface `miss_enabled` and `review_mode` as real process-page controls**
   ("Find misses" toggle; "Write species review results" control). A saved
   process is then literally the page state — no hidden fields.
3. **Global library, per-workspace default.** One shared list of processes;
   *which one is the default* is remembered per workspace (as today).
4. **DB table with stable integer IDs.** Everything that points at a process
   (workspace default, import chaining, job provenance) references it by **id**,
   so renaming never breaks a reference. Deleting a process nulls out any
   workspace that defaulted to it (→ "import only").
5. **Process page = editor** (explicit Save / Save as new / Rename / Delete,
   with a "modified (unsaved)" state; Run always uses current toggle values).
   **Import page = pure picker** (no toggles).

## Data model

New global SQLite table `saved_processes`:

| column | type | notes |
|---|---|---|
| `id` | INTEGER PK AUTOINCREMENT | stable identity across renames |
| `name` | TEXT UNIQUE NOT NULL | display name |
| `skip_classify` | INTEGER (0/1) | ↔ "Classify" toggle |
| `skip_extract_masks` | INTEGER | ↔ "Extract Features" |
| `skip_eye_keypoints` | INTEGER | ↔ "Eye Keypoints" |
| `skip_regroup` | INTEGER | ↔ "Group & Score" |
| `miss_enabled` | INTEGER | ↔ new "Find misses" toggle |
| `review_mode` | TEXT NULL | `'species'` or NULL ↔ new control |
| `is_seed` | INTEGER | marks the four shipped seeds (labeling only) |
| `sort_order` | INTEGER | list ordering on the process page |

- **App-wide default:** stored in global config as `pipeline.default_process_id`
  (int, nullable). Points at the seed used for fresh workspaces / import UI
  default.
- **Per-workspace default:** replaces `pipeline.default_strategy` (a string) with
  `pipeline.default_process_id` (int) in `workspaces.config_overrides`.
  NULL/absent = "import only, no processing".

### Seeds (`process_strategies.SEED_PROCESSES`)

`_BASE` defaults stay as the "everything on" baseline (column defaults for a
brand-new process). The four seeds keep today's semantics:

- **Identify birds** — `skip_extract_masks, skip_eye_keypoints, skip_regroup`;
  `miss_enabled=False`; `review_mode='species'`. (App default.)
- **Full** — all stages on.
- **Cull-ready** — `skip_extract_masks, skip_eye_keypoints`; `miss_enabled=False`.
- **Quick look** — everything off except thumbnails/previews
  (`skip_classify` + the rest); `miss_enabled=False`.

### Migration (one-shot, db_meta-guarded)

Because the live DB's `PRAGMA user_version` can be **ahead** of `main` (parallel
branches), a `user_version < N` gate would silently skip on the real DB. Guard
with **`db_meta` markers** instead (like `eye_kp_fingerprint_backfill`):

- `saved_processes_seeded`: insert the four seeds once. Never re-seed (so a user
  who deletes all processes doesn't get them back).
- `default_strategy_to_process_id`: map any existing per-workspace
  `pipeline.default_strategy` string and the global `default_strategy` to the
  corresponding seed's new id; unknown/removed names → null.

Seeding uses `CREATE TABLE IF NOT EXISTS` (safe regardless of version).

## API

New CRUD (global; no workspace scope):
- `GET /api/processes` — list (ordered by `sort_order, id`).
- `POST /api/processes` — create `{name, skip_*, miss_enabled, review_mode}`.
- `PUT /api/processes/<id>` — update / rename.
- `DELETE /api/processes/<id>` — delete + null-out referencing workspaces +
  clear the global default if it pointed here.

Changed:
- `POST /api/jobs/pipeline` and the plan endpoint: drop the `strategy: <name>`
  path; accept `process_id: <int>` (expanded server-side) **and** still accept
  explicit stage flags (what the process page's Run posts, now including
  `miss_enabled` and `review_mode`).
- `_enqueue_process_job(strategy_name=...)` → `process_id=...`; resolves flags
  from the row.
- `_validate_after_import`: value is null or an existing `process_id` (DB check).
- `_validate_workspace_config_overrides`: validate `pipeline.default_process_id`
  is null or an existing id, instead of validating a strategy name.
- `api_settings_schema`: inject the live process list as the enum/enum_labels for
  `pipeline.default_process_id` so the Settings widget renders a picker. The
  workspace-PATCH endpoint validates the id against the DB (config_schema stays
  DB-agnostic: the stored spec is `int`+`nullable`, existence checked at the
  endpoint).

`process_strategies.resolve_strategy(name)` is removed; resolution becomes
`db.resolve_process(process_id) -> flags dict` (or `get_saved_process` +
`_row_to_flags`).

## Process-page editor (`pipeline.html`)

- `strategySelect` → **saved-process picker** populated from `GET /api/processes`
  (option value = id). Adjacent buttons: **Save**, **Save as new…**, **Rename**,
  **Delete**. Selecting a process loads its six values into the toggles.
- Two new controls so the page is the complete truth:
  - **Find misses** ↔ `miss_enabled` (disabled/greyed with a hint when Classify
    or Group is off, mirroring the real stage gate).
  - **Write species review results** ↔ `review_mode` (only meaningful when
    Classify on + Group off — the `identify` shape; hidden/inert otherwise).
- Changing any toggle flips the picker to **"• modified (unsaved)"**. **Run**
  always uses current toggle values (one-off tweak without saving). **Save**
  overwrites; **Save as new…** prompts a name; **Rename**/**Delete** act on the
  selection. No process selected = **"Custom (unsaved)"** (today's "Custom (use
  toggles)").
- `applyStrategyPreset`'s hardcoded preset table is removed; presets now come
  from the fetched rows.

## Import page (`import.html`) & Settings

- Import "After import" `<select>` populated from `GET /api/processes` (by id) +
  a fixed "Import only — no processing" option (null). No toggles.
- Settings per-workspace default renders as a picker over the same list + "Import
  only", via the injected schema enum.

## Testing

- `vireo/tests/test_db.py`: create/list/update/delete; `name` uniqueness;
  delete nulls referencing workspaces + global default; seed insertion is
  idempotent (no double-insert, no re-seed after delete-all);
  `resolve_process` round-trips all six fields.
- `vireo/tests/test_processes_api.py` (new): the four CRUD endpoints (happy +
  400 bad flags + 404 missing id + duplicate-name); `/api/jobs/pipeline` with
  `process_id` expands and still accepts explicit flags; workspace default
  accepts valid/null id and 400s on a bad one.
- Migration test: old `default_strategy: "identify"` override maps to the seeded
  id; unknown names → null.

## Out of scope (YAGNI)

- Building *new* import→process chaining (already wired; we just re-point it at
  ids).
- Sharing/export of processes, multi-user concerns, drag-reorder UI beyond a
  simple list.
