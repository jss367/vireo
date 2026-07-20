# Universal Photo Filters — Implementation Plan

Companion to `2026-07-19-universal-filters-design.md`. Five phases, one PR
each. Every phase lands green: `python -m pytest tests/test_workspaces.py
vireo/tests/test_db.py vireo/tests/test_app.py vireo/tests/test_photos_api.py
vireo/tests/test_edits_api.py vireo/tests/test_jobs_api.py
vireo/tests/test_darktable_api.py vireo/tests/test_config.py -v` plus new
tests, plus a Playwright drive of the affected page.

## Phase 1 — Backend: rule-engine extension + field registry (PR 1)

All in `vireo/db.py` (`_build_query_from_rules`, 17617) and `vireo/app.py`
unless noted.

1. **EXIF column promotion.** Add columns `camera_make, camera_model, lens,
   aperture, shutter_speed, iso` to `photos`; backfill from `exif_data` JSON
   in a `db_meta`-marker migration (pattern: `repair_duplicate_photo_species`
   guard, db.py 10821 — not user_version, which has drifted). Extract on
   ingest going forward.
2. **New rule fields** in `_build_query_from_rules`: `filename` (contains/
   is/starts/ends, case toggle), `file_size`, `width`, `height`,
   `focal_length`, the six EXIF fields, `gps_lat`/`gps_lng`, `has_edits`
   (EXISTS `photo_edit_recipes`), `has_visual_index` (EXISTS
   `photo_embeddings` for active model), `burst_id`, `duplicate_group`
   (= `file_hash`), `species` (EXISTS via keyword/taxa path,
   `get_species_keywords_for_photos` semantics, db.py 12379).
3. **New ops**: `in`/`not_in` (enum multi-select); generalize `between`
   beyond timestamp; `recent {n, unit}` for date fields. Validation rejects
   unknown fields/ops/value shapes with 400s.
4. **Field registry**: a module-level `FILTER_FIELDS` dict (label, category,
   type, ops, enum values, suggest flag, SQL binding) consumed by both the
   query builder and `GET /api/filters/fields`.
5. **Endpoints**: `POST /api/photos/query` (rules + sort + paging → list +
   total, workspace-scoped exactly like `get_photos`); `GET
   /api/filters/fields`; `GET /api/filters/values` (distinct values + counts
   for suggest fields; expression-minus-edited-rule semantics — drop the
   clause from its group, never substitute true).
6. **Tests** (`vireo/tests/test_db.py`, `test_photos_api.py`): each new
   field/op; any/none group nesting; values endpoint counts under sibling
   filters; migration backfill idempotence; validation failures.

Nothing user-visible changes in this phase.

## Phase 2 — Shared filter bar on Browse (PR 2)

1. `vireo/templates/_filterbar.html` + `vireo/static/vireo-filter.js`
   (IIFE on `window`, pattern: `VireoTextSearch`, vireo-utils.js:86).
   Port the prototype's interaction model: quick search (single replaceable
   clause + suggestion menu), chips row with locked scope chip and `+N`
   overflow, popover with quick filters (rating comparator, multi-select
   flags/colors), rule builder (field picker, per-type ops, typeahead
   values with counts via `/api/filters/values`, enum count pills,
   between/relative-date inputs), Advanced toggle (never rewrites the tree),
   pause on `\`, undo toasts. Reuse prototype JS logic where it transfers;
   all evaluation moves server-side.
2. Wire Browse: replace `buildCurrentBrowseParams()`/inline widgets
   (browse.html:3989) with the shared module calling `/api/photos/query`.
   Legacy URL params (deep links) compile to an initial rule tree.
3. Persistence: active expression + muted per page in `workspaces.ui_state`.
4. Playwright: port the prototype's `verify_features.py` checks against the
   real Browse page (typeahead counts, multi-select, relative date, between,
   pause, chip labels).

## Phase 3 — Visual (CLIP) clause (PR 3)

1. `POST /api/photos/query` accepts `visual {prompt, strength}`; pipeline:
   rule-tree candidate ids → `/api/photos/search` internals
   (app.py 25807: active model, `get_photos_with_embedding`, text encode,
   cosine, threshold by strength broad/balanced/strict) → ordered ids +
   similarity.
2. Error states surfaced in the response (`no_model`,
   `model_no_text_search`, `no_embeddings`) → error chip + "metadata filters
   shown only" badge; metadata rules still apply.
3. Relevance sort only while the visual clause is active and healthy and
   filters are not paused. Facet counts include the visual result set when
   healthy.
4. "Visual index: N of M photos" badge from embedding coverage.

## Phase 4 — Map, Review, Duplicates adoption (PR 4)

1. Map: replace `/api/photos/geo` param variant with `/api/photos/query` +
   Map scope (GPS-or-location-keyword predicate from
   `get_geolocated_photos`, db.py 7583). Scope chip; full field set now
   works on Map (closing today's gap: no color/flag/collection filters).
2. Review: scope = pending `prediction_review`; page-specific fields
   (`prediction`, `prediction_confidence`, `prediction_status`) appear in
   the picker only here (registry `pages` attribute).
3. Duplicates: filter selects matching members; groups render complete with
   "Matches filter" badges (prototype behavior), via `file_hash` grouping.
4. "Open results in…" handoff: serialize expression, navigate, destination
   adds its scope chip.

## Phase 5 — Save-as-collection + legacy cleanup (PR 5)

1. Save the current expression as a collection (rules JSON is already the
   collection format). Save preview shows the post-save count (paused state
   never inflates it). Opening a collection loads its rules into the filter
   bar as editable chips.
2. Delete the duplicated Browse-param filter blocks (`get_photos`,
   `get_photo_ids`, `count_filtered_photos`, `get_browse_summary`,
   `get_geolocated_photos` variant) once all callers use the rule path;
   keep thin param→rules shims only where external deep links need them.
3. Docs: update README/help for the filter bar and `\` shortcut.

## Review-cycle regressions to guard (from prototype PR #1319)

- Facet-count helper must drop the edited clause from its group
  (any/none semantics), not treat it as true.
- Group Match select routes through the same handler as rule edits — guard
  clauses must not swallow it (regression af0a0d2b).
- Debounced edits must be cancelled on tree mutation.
- `none` group summaries join with OR inside NOT.
- Saved state must strip `muted`; previews show post-action counts.
- `escapeHtml` must escape quotes (attribute injection).
