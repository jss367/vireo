# Edit presets

## Motivation

Recurring shooting situations ("high-ISO forest", "backlit water", "overcast
plumage") get re-dialed by hand for every photo. Copy Settings already moves a
recipe between photos in one session; presets make that vocabulary persistent
and one click from the editor.

## Decisions

- **Adjustments-only.** A preset stores the `adjustments` object (tone, white
  balance, detail) and never geometry — rotation, flip, straighten, and crop
  are facts about one photo, not a look. The server strips geometry on save.
- **Global, not workspace-scoped.** Presets are the photographer's vocabulary,
  like photos and keywords; a look is the same look in every workspace.
- **Upsert by name.** Saving a preset under an existing name overwrites it —
  the natural "tweak and re-save" loop, no versioning ceremony.
- **Apply replaces the whole adjustments object** (geometry untouched). The
  mental model is "make the sliders look exactly like when I saved this",
  which is transparent and idempotent, rather than a per-key merge whose
  result depends on the photo's current state.
- Applying only edits the in-editor working recipe (marks dirty, re-renders
  preview); nothing persists until Save. Batch application rides the existing
  Copy Settings / bulk-apply flows.

## Storage

```sql
CREATE TABLE IF NOT EXISTS edit_presets (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    name        TEXT NOT NULL UNIQUE,
    recipe_json TEXT NOT NULL,           -- canonical {"version":1,"adjustments":{...}}
    created_at  TEXT NOT NULL DEFAULT (datetime('now')),
    updated_at  TEXT NOT NULL DEFAULT (datetime('now'))
);
```

`recipe_json` is validated and canonicalized through `normalize_recipe` (same
path as photo recipes), restricted to the adjustments section. A preset that
normalizes to no adjustments is rejected, not stored empty.

## API

- `GET /api/edit-presets` → `{presets: [{id, name, recipe, updated_at}, ...]}`
  sorted by name.
- `POST /api/edit-presets` `{name, recipe}` → upsert by trimmed name; strips
  geometry, 400 on invalid/empty adjustments or blank/overlong name.
- `DELETE /api/edit-presets/<id>` → 404 if unknown.

No rename endpoint in v1 — save under the new name and delete the old one.

## Editor UI

A **Presets** band above Adjustments in `photo_editor.html`: a `<select>` of
saved presets plus Apply / Save… / Delete buttons. Save prompts for a name
(prefilled with the current selection) and captures the current adjustments
(both the Adjustments and Detail bands). Apply overwrites the working
recipe's adjustments, syncs sliders, and re-renders the preview.

## Follow-ups (out of scope)

- Grid right-click "Apply preset" batch action (merge-with-geometry endpoint).
- Preset preview thumbnails.
