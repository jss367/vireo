# Settings control — design

Date: 2026-04-25
Branch: `settings-control`

## Problem

Three pain points the user wants solved (sharing presets is explicitly out of scope):

1. **Backup / portability.** Settings should survive a reinstall and live alongside dotfiles.
2. **Discoverability / depth.** Many `DEFAULTS` keys (pipeline weights, miss-detection gates, eye-focus tuning, burst tuning) have no UI today. "Full control" currently means hand-editing JSON.
3. **Visibility.** No way to see what's actually in effect right now (default vs. global vs. workspace) or reset a single knob.

The current `settings.html` is 2482 lines of hand-rolled HTML across 22 sections. Adding a knob means writing more HTML, which is why the long tail is missing.

## Direction

Take VS Code's settings model — schema-driven UI, search-first navigation, per-field provenance + reset, raw-JSON escape hatch — and size it down to one app.

What we steal:
- Two synced views (pretty UI + raw JSON) on one page.
- Search-first navigation rather than nested menus.
- Schema-in-code as the single source of truth for widget rendering and validation.
- Per-field "default / global / workspace" badge + reset-to-default.
- "Edit raw JSON" escape hatch, doubling as the export/import surface.

What we skip:
- A formal JSON Schema spec for editor autocomplete (unnecessary for a single-app config).
- Settings Sync (dotfiles already cover this).
- Sanitization / preset sharing (sharing is out of scope).

## Architecture

### Three layers, in order of authority

```
default (DEFAULTS in code)
  ↓ overridden by
global  (~/.vireo/config.json)
  ↓ overridden by (when applicable)
workspace (workspaces.config_overrides JSON column)
```

This is unchanged — `db.get_effective_config()` already merges in this order. We are adding **UI** that makes each layer visible and editable per-key.

### Page layout

Top of `settings.html`:

- Sticky toolbar:
  - Search box (filters live by key + label + description).
  - Scope tab strip: `Global · Workspace: <active>`. Switching the tab switches which layer edits write to and which provenance badges are shown.
  - Action menu: `Export…`, `Import…`, `Open raw JSON`, `Reset all to defaults`.

Body:

- The existing 22 hand-rolled sections, unchanged, in their current order.
- A new **"All settings"** region rendered from a schema. Grouped by category (Pipeline, Detection, Culling, Display, Working copy, Preview, Ingest, Paths, Integrations). Includes *every* key — including the ones the curated sections also expose, so search always finds it.

Raw-JSON view: separate sub-page or modal. Monospace textarea, validate-on-save, import/export buttons.

## Schema

New module `vireo/config_schema.py` holds metadata in parallel to `DEFAULTS`. Keeping it separate (not extending `DEFAULTS` in place) preserves backward compatibility with every existing `cfg.load().get("…")` call site.

```python
# vireo/config_schema.py
SCHEMA = {
  "classification_threshold": {
    "type": "float", "min": 0.0, "max": 1.0, "step": 0.01,
    "category": "Detection",
    "scope": "both",
    "label": "Classification threshold",
    "desc": "Minimum confidence for a species prediction to be kept.",
  },
  "hf_token": {
    "type": "secret", "category": "Integrations",
    "scope": "global",
    "label": "Hugging Face token",
    "desc": "API token used for model downloads.",
  },
  "pipeline.w_focus": {
    "type": "float", "min": 0.0, "max": 1.0, "step": 0.01,
    "category": "Pipeline",
    "scope": "both",
    "label": "Focus weight",
    "desc": "Pipeline scoring weight for focus quality.",
  },
  ...
}
```

### Field meanings

- `type`: `int | float | bool | string | secret | enum | path | list[string]`. Drives widget choice.
- `min` / `max` / `step` / `enum`: optional, type-specific.
- `category`: section header in the schema-rendered region; also the search facet.
- `scope`: `"global" | "workspace" | "both"`. Controls whether the per-workspace override toggle shows on the row.
- `label` / `desc`: human strings; `desc` is search-indexed.

### Nested keys

Use dotted paths (`pipeline.w_focus`). Two helpers:

- `get_dotted(d, "pipeline.w_focus") -> 0.45`
- `set_dotted(d, "pipeline.w_focus", 0.5)` — creates intermediate dicts as needed.

### Drift guard

Unit test asserts `set(SCHEMA.keys()) == set(flatten(DEFAULTS).keys())`. Adding a key to `DEFAULTS` without a schema entry breaks the build — the mechanism that makes "every knob is discoverable" enforced rather than aspirational.

### Validation

One function used by every write path and by import:

```python
def validate_value(key, raw):
    spec = SCHEMA[key]
    coerced = coerce(raw, spec["type"])     # int(...), float(...), bool, ...
    check_range(coerced, spec)              # min/max/enum
    return coerced  # raises ValueError with a useful message on failure
```

## Per-row UX

```
[Category: Pipeline] ──────────────────────
  ● Focus weight                    [0.45]   ⟳
    Pipeline scoring weight for focus quality.
    [● Per-workspace override]

  ○ Exposure weight                 [0.20]   ⟳
    Pipeline scoring weight for exposure.
```

- **Provenance dot** at left:
  - `○` empty (grey) — current value equals default.
  - `●` blue — differs from default at the global layer.
  - `●` purple — differs from global at the workspace layer (only shown on the Workspace scope tab).
- **Widget**: number input with min/max for floats/ints, toggle for bools, dropdown for enums, text for strings, masked + "Reveal/Copy" for secrets.
- **`⟳` reset icon** on hover: reverts that one key to its default (or for the workspace tab, deletes the workspace override).
- **Description** under the row, always visible.
- **Per-workspace toggle** below the row, only when `scope: "workspace" | "both"` and the scope tab is "Global". Clicking copies the current global value into the workspace override and switches the row to workspace context.

**Search** filters by category, label, key, and `desc`. Empty categories collapse out. Enter on a single match focuses the widget.

**Saving:** debounced auto-save per row (300ms after last keystroke / on blur for inputs, immediate for toggles). Per-row indicator: `Saving… ✓ Saved`. No global Save button — VS Code parity.

**Curated sections at the top** keep their current behavior unchanged. We do not retrofit per-row reset/badge into them — they're hand-built UX and adding that complexity would be inconsistent. Search ignores curated sections; the schema-rendered region below covers their keys, so every key remains findable.

## Backend

New endpoints in `vireo/app.py`:

```
GET  /api/settings/schema
     → { "schema": SCHEMA, "categories": [...] }

GET  /api/settings/values
     → { "default": {...}, "global": {...}, "workspace": {...},
         "effective": {...} }
     # All four layers, dotted-flat. UI uses these to render badges.

PATCH /api/settings/global
     body: { "key": "pipeline.w_focus", "value": 0.5 }

DELETE /api/settings/global/<dotted-key>

PATCH /api/settings/workspace
     body: { "key": "...", "value": ... }
     # 400 if scope == "global".

DELETE /api/settings/workspace/<dotted-key>

POST /api/settings/import
     body: { "json": "<file contents>" }
     # Validates entire payload against schema.
     # On success: atomically replaces ~/.vireo/config.json.
     # Workspace overrides untouched.
     # Returns 400 with per-key errors on validation failure.

GET /api/settings/export
     → application/json download of current ~/.vireo/config.json
```

### Atomicity

`cfg.save()` already writes via tempfile + `os.replace`, so import inherits atomic-replace semantics. Workspace override writes go through `db.set_workspace_config_override(key, value)`.

### Import semantics

Replace global wholesale. Import only touches `~/.vireo/config.json`. Active workspace overrides survive — that's the "restore from backup" mental model: backups capture global state, workspaces are local state.

(Possible future extension: a "Full backup" mode that round-trips workspace overrides too. Out of scope for v1.)

### Scope guards

- `PATCH /api/settings/workspace` rejects keys whose `scope == "global"` (e.g. `hf_token`, `report_url`).
- `PATCH /api/settings/global` accepts every key.

## File changes

- **New:** `vireo/config_schema.py` — `SCHEMA` dict, `validate_value`, dotted-path helpers.
- **New:** `vireo/templates/_settings_schema.html` — partial that loops over the schema and renders category sections, included from `settings.html`.
- **Edit:** `vireo/templates/settings.html` — toolbar (search, scope tabs, action menu) at top; include the new partial below the 22 existing sections; raw-JSON modal.
- **Edit:** `vireo/app.py` — the seven endpoints above.
- **Edit:** `vireo/db.py` — add `set_workspace_config_override(key, value)` / `delete_workspace_config_override(key)` if not already present. The merging side already exists in `get_effective_config`.

## Build order

Each step independently shippable:

1. Land `config_schema.py` + drift-guard test. No UI yet. Schema and validators exist; test asserts key parity with `DEFAULTS`.
2. Land read endpoints (`/schema`, `/values`) and the search-first schema-rendered region in `settings.html`. **Read-only at first** — every key visible with provenance badges, edits still go through curated sections. This alone solves #3 and #4.
3. Land write endpoints + per-row editing + reset. Auto-save, debounced.
4. Land per-workspace override toggle and the Workspace scope tab.
5. Land raw-JSON tab + import/export. This solves #1.

## Testing

- Drift guard: `set(SCHEMA.keys()) == set(flatten(DEFAULTS).keys())`.
- Validator unit tests: type coercion (`"0.5"` → `0.5`), range rejection, enum rejection, secret/path treated as strings.
- Endpoint tests in `vireo/tests/test_settings_api.py`: PATCH/DELETE round-trip, scope guard rejects global-only key on `/workspace`, import-replace preserves workspace overrides, import rejects malformed JSON.
- No browser tests in scope. Manual smoke per CLAUDE.md "user-first testing" before shipping.

## Out of scope (deferred)

- Migrating any of the 22 curated sections into the schema-rendered region.
- Settings sync / multi-machine sync (dotfiles cover this).
- Public sharing / preset library (sharing was explicitly excluded).
- Bundling workspace overrides in export/import (could be added later as a "Full backup" mode).
