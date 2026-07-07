# Life List Explorer — taxonomic completeness explorer

**Date:** 2026-07-07
**Status:** Design approved, ready for implementation plan

## Goal

Let the user visually explore how complete their life list is at every
taxonomic level. For a chosen class (birds by default), show how many
**orders / families / genera / species** exist in total and how many they
have **found** (photographed and tagged), with drill-down from orders all the
way to a found+missing species checklist. It should look really nice.

## Key finding that shapes everything

Vireo's `taxa` table already holds the **complete iNaturalist reference
taxonomy** (Animalia/Plantae/Fungi, hundreds of thousands of taxa), fully
linked parent→child via `parent_id` from species up to kingdom
(`vireo/db.py:495`, `vireo/taxonomy.py:441`). So the **denominator** ("how many
bird orders/families/etc. exist") is a local query — no external checklist to
source or ship. The local taxonomy *is* the world checklist.

- **Denominator** = reference taxa at each rank under the chosen root class.
- **Numerator** = tagged species (`keywords.taxon_id`) walked up their
  `parent_id` chains, deduped per rank.

Two real dependencies the UI must handle honestly (No-black-boxes rule):
- The user must have run the **download-taxonomy** job, or there is no
  denominator.
- Tagged species with `taxon_id IS NULL` cannot be placed in the tree and must
  not silently undercount.

## Decisions (all approved)

1. **Scope:** any class, **Birds (Aves) as default**; a class selector lets the
   user switch to Mammalia, Insecta, etc. (only classes they have tagged
   species in).
2. **Visual paradigm:** drill-down **cards with progress rings** as the
   backbone; a **sunburst** showpiece on top. Cards are the source of truth for
   exact "X of N"; sunburst is the wow-factor / whole-shape overview.
3. **Leaf experience:** species leaf shows **found + missing** — found lit with
   a thumbnail, missing dimmed with a "not yet" state (gap-finder).
4. **Honesty states surfaced explicitly:** taxonomy-not-downloaded CTA; and an
   unmatched-species footnote/list for tagged species with no `taxon_id`.
5. **Placement:** a **tab on `/life-list`** — "List" (today's view) and
   "Explorer" (new). Tab state in the URL (`?view=explorer`).
6. **Data scope:** **match whatever the existing life list uses**
   (`_build_life_list_payload()` / `get_life_list_candidates()`), so Explorer
   and List always agree on the same numerator/denominator. Confirm exact scope
   (workspace vs global) during planning.

## Backend

New endpoint: `GET /api/life-list/explorer?root=<taxon_id|Aves>` returning the
completeness tree for one class.

Computation (all from the local `taxa` tree + the user's tags):
1. **Denominator** — count reference taxa at each rank among the root class's
   descendants; precompute each node's descendant-species set so any node knows
   its total.
2. **Numerator** — tagged species (`keywords.is_species=1 AND taxon_id NOT
   NULL`, scoped to match the existing life list); walk each up `parent_id`,
   marking every ancestor found and tallying found-species-per-node.
3. **Per node** payload: `name`, `common_name`, `rank`, `found_count`,
   `total_count`, `pct`, children.

Payload shape:
- One call returns the class tree down to **genus** with counts (orders →
  families → genera is a few thousand nodes for birds — fine in one response).
- **Species leaves load per-genus** on drill-in via `?parent=<genus_id>` to keep
  responses light; leaves return **found + missing**, found ones with a
  best-photo thumbnail id.

Honesty fields in the payload:
- `taxonomy_ready: false` when no reference taxa exist under the root → UI shows
  download-taxonomy CTA.
- `unmatched_species` — count + list of tagged species with `taxon_id IS NULL`.

DB: add methods in `db.py` (e.g. `get_taxon_completeness(root, ...)`) using a
recursive CTE over `parent_id`. Tests in `vireo/tests/`.

## Frontend

`/life-list` gains two tabs — **List** (today's view, untouched) and
**Explorer**. Tab state in URL (`?view=explorer`), linkable/refresh-safe.

Explorer layout, top → bottom:
1. **Class selector** — defaults to Birds (Aves); lists classes the user has
   tagged species in. Switching re-roots everything.
2. **Sunburst showpiece** — inner ring orders → families → genera → outer
   species; arcs shaded by % found (filled = found, ghosted = missing), sized by
   species count. Hover tooltip "X/N · %"; click a wedge zooms and drives the
   drill-down below. Lightweight inline SVG/canvas, no heavy library (matches
   Vireo's vanilla-JS / inline-style convention).
3. **Breadcrumb** — Birds › Passeriformes › Passerellidae, each crumb clickable.
4. **Card grid** — cards for the current level (orders, then families, then
   genera), each with a **progress ring** + "38/71 families · 54%". Click drills
   down.
5. **Species leaf** — found+missing checklist; found with thumbnail, missing
   dimmed with a "not yet" tag.

States (No-black-boxes):
- Taxonomy not downloaded → full-panel CTA with the existing download-taxonomy
  job button.
- Unmatched-species footnote with click-through list.
- Empty class → honest "No tagged species in this class yet."

## Rollout (each stage independently shippable)

1. Backend endpoint + DB method + tests.
2. Tab + card-grid drill-down + species leaf + honesty states (the fully-usable
   core).
3. Sunburst showpiece as a second pass on top.

## Tests

- DB completeness math: found/total/pct, ancestor rollup, dedup across multiple
  species sharing an ancestor.
- Endpoint states: ready / not-ready / unmatched-species present.
- Scope matches the existing life list.
