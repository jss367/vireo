# Universal Photo Filters — Design

Status: validated by interactive prototype (`docs/plans/2026-07-19-photo-filter-prototype/`, PR #1319, merged 2026-07-19).

## Goal

One holistic filter pattern applied identically to every photo-listing surface
(Browse, Map, Review, Duplicates, and future pages): a quick-search box, a row
of readable filter chips with a locked page-scope chip, and a popover holding
quick filters (rating/flag/color), a rule builder over all metadata, and
optional nested AND/OR/NONE logic. CLIP visual search is a first-class,
visually distinct clause with honest error states.

## Core decision: build on the smart-collection rule engine

Vireo already has a universal filter DSL: `_build_query_from_rules`
(vireo/db.py:17617), used by collections. It supports a nested
`{"mode": "all|any|none", "rules": [...]}` tree, ~35 fields, workspace
scoping, and a live-count preview endpoint (`POST /api/collections/preview`).

The universal filter **is** this rule tree. Consequences:

- "Save as Collection" is free — a saved filter is just a collection row
  (`collections.rules` already stores this JSON).
- Browse/Map/Review/Duplicates and collections converge on one SQL path.
- The copy-pasted Browse-param filter block (duplicated across `get_photos`,
  `get_photo_ids`, `count_filtered_photos`, `get_browse_summary`, plus a fifth
  variant in `get_geolocated_photos`) gets replaced, fixing today's
  inconsistency where Map accepts fewer filters than Browse.

## Filter expression model

```json
{
  "mode": "all",
  "rules": [
    {"field": "rating", "op": ">=", "value": 4},
    {"mode": "any", "rules": [
      {"field": "flag", "op": "is", "value": "pick"},
      {"field": "color_label", "op": "in", "value": ["red", "yellow"]}
    ]}
  ]
}
```

Extends the existing engine with (all validated server-side):

- `in` / `not_in` ops (multi-select) for enum-like fields.
- `between` for numbers and dates (engine already has `between` for
  timestamp; generalize).
- `recent_days` already exists; add `recent` with `{n, unit}` for
  days/weeks/months/years ("is in the last").
- New fields (columns exist, engine support doesn't): `filename`,
  `file_size`, `width`, `height`, `focal_length`, `gps_lat`/`gps_lng` range,
  `has_edits` (EXISTS on `photo_edit_recipes`), `has_visual_index` (EXISTS on
  `photo_embeddings` for the active model), `burst_id`, `duplicate_group`
  (equality on `file_hash`).
- EXIF camera fields (`camera_make`, `camera_model`, `lens`, `aperture`,
  `shutter_speed`, `iso`): promoted from `photos.exif_data` JSON to real
  columns via a backfill migration (db_meta marker, not user_version — see
  drift note in memory). Until promoted these have no efficient backing.

The **visual clause** is not part of the rule tree. It rides alongside:

```json
{"visual": {"prompt": "bird flying at dusk", "strength": "balanced"}}
```

and reuses the `/api/photos/search` flow (candidate ids from the rule tree →
embedding similarity → threshold by strength). Error states map to existing
detections: `no_model`, `model_no_text_search` (timm), `no_embeddings` — the
UI shows the clause in an error state and applies metadata rules only,
exactly as prototyped. Never silently return zero.

## Page scope

Each page contributes a fixed, non-removable scope ANDed onto the user
expression, rendered as a locked chip:

| Page | Scope |
|---|---|
| Browse | workspace folders (status ok/partial) — the implicit baseline |
| Map | has GPS or a location keyword (today's `get_geolocated_photos` predicate) |
| Review | photos with predictions whose effective review status is `pending` — i.e. `COALESCE(prediction_review.status, 'pending') = 'pending'`, so the many predictions that have no `prediction_review` row (the default-pending storage model — see the `COALESCE(pr_rev.status, 'pending')` predicates already used across `db.py`) are included, not dropped |
| Duplicates | photos whose `file_hash` groups ≥2; a matching member reveals its whole group |

"Open results in…" hands the user expression to another page; the destination
adds its own scope chip.

## API surface

- `GET/POST /api/photos/query` — rule tree + visual clause + sort + paging →
  photo list + total. Replaces the per-page param soup; legacy params compile
  to rule trees internally during migration.
- `GET /api/filters/fields` — the field registry (label, category, type, ops,
  enum values, suggest flag), server-defined so the UI picker and validation
  share one source of truth.
- `GET /api/filters/values?field=&q=&rules=` — typeahead facet values with
  counts. Counts are computed against the expression **minus the rule being
  edited** (drop the clause from its group — not "treat as true", which breaks
  any/none groups; prototype review finding) and **including** an active,
  healthy visual clause.

## UI

New shared assets following the established pattern (`vireo/static/` IIFE
modules + a Jinja include):

- `vireo/templates/_filterbar.html` — markup include.
- `vireo/static/vireo-filter.js` — state, chips, popover, rule rows,
  typeahead, keyboard (`⌘F` focus, `\` pause).
- Pages adopt it and delete their inline filter widgets.

Behavioral requirements carried from the prototype and its review cycle
(these are hard requirements per CORE_PHILOSOPHY "no black boxes"):

1. Chip text must state exact semantics ("Rating is at least 4 stars",
   "Flag is one of Picked, Unflagged", "NOT (A OR B)" for none-groups —
   never "NOT (A AND B)").
2. Quick search is one replaceable clause; multiple text rules are a
   deliberate Add-filter action.
3. Quick rating shows its comparator (≥/=/≤); flags and colors multi-select
   into `is one of` rules.
4. Pause (`\`) disables filters without losing them: dimmed dashed chips,
   "N would match" badge, persists, and any save/preview shows the count the
   action will actually produce — not the paused view's count.
5. Toggling Advanced logic off never rewrites the rule tree.
6. Relative dates (`is in the last N days/weeks/months/years`) and `between`
   are single rules.
7. Facet counts must answer "how many results would I get" under the current
   selections — never a global COUNT(*).

## Persistence

Active filter per page per workspace in `workspaces.ui_state` JSON
(written via existing `PUT /api/workspaces/<id>`), including the `muted`
flag.

Saved filters are collections, but because the visual clause is
intentionally not part of the rule tree, `collections.rules` alone can't
represent an expression with a visual component — a collection saved from
such an expression would silently reopen as metadata-only. To avoid that,
the collections schema gains a sibling `visual_json` column (nullable
JSON: the same `{prompt, strength}` payload as the query envelope), and
both the save-as-collection endpoint and the collection-open flow round-
trip the pair `{rules, visual}` together. `_build_query_from_rules` keeps
reading `rules`; the visual clause is applied by the same query pipeline
that handles the live filter bar, so an opened collection with a visual
clause behaves identically to the expression that produced it. Collection
listings surface a visible marker (e.g. a small "visual" chip) so users
know the clause is active before running it.

## Species and multi-species

The `species` filter field uses the keyword path (`photo_keywords` →
`keywords` where `is_species=1 OR type='taxonomy'`, deduped by `taxon_id`
per the multi-species model) with EXISTS predicates — a photo with several
species matches if **any** species matches. Predictions
(`predictions` + `prediction_review`) are a separate field axis
(`prediction`, `prediction_confidence`, `prediction_status`), as in the
prototype.

## Out of scope (for now)

- Lightroom-style multi-column metadata browser (facet columns). The
  typeahead-with-counts covers the need; a column browser can layer on the
  same `/api/filters/values` endpoint later.
- ANN indexing for embeddings (brute-force matmul is fine at current scale).
- Filter presets distinct from collections.
