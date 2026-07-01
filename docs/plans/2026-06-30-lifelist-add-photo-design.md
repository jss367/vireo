# Easier "Add to Life List" — design

**Date:** 2026-06-30
**Branch:** `lifelist-add-photo`

## Problem

The only way to choose which photo represents a species in the life list is:
open `/life-list` → click a species card → the lightbox opens → find the shot →
click "Use as Life List". The action lives only on the life-list page, so
crowning a lifer means detouring away from wherever you were actually looking at
photos.

Note: a photo is never "added to the life list" directly. Any photo tagged with a
species keyword (`is_species = 1` or `type = 'taxonomy'`) is automatically in the
list. The only manual choice is **which photo represents each species** — stored
in `photo_preferences` (`purpose = 'life_list'`, keyed by workspace + purpose +
species).

## Goal

Bring the "choose the representative photo" action to where the user already is:

1. The **shared lightbox** (works on every page via `_navbar.html`).
2. The **browse right-click context menu**.

Chosen wording: **"Add to Life List"** (matches how the user phrases it), flipping
to a selected **"★ Life List photo"** state once the shot is the representative, so
the control still reflects real current state.

## Section 1 — Backend: `life_list` block on `GET /api/photos/{id}`

Add a `life_list` array to the existing per-photo payload the lightbox already
fetches:

```json
"life_list": [
  { "species": "American Robin", "is_current_photo": false },
  { "species": "Blue Jay",       "is_current_photo": true  }
]
```

- **Eligible species** = the photo's keywords matching `is_species = 1 OR
  type = 'taxonomy'` — the exact rule `get_life_list_candidates` uses — intersected
  with the active workspace's visible folders (`workspace_folders`). A species in a
  folder not visible to the active workspace is not in this workspace's list, so it
  does not appear.
- **`is_current_photo`** = a `photo_preferences` row exists with
  `purpose = 'life_list'`, that species, and this `photo_id`.
- Empty array → no lifelist affordance anywhere for this photo.

Build this from the same eligibility SQL as the list itself so the button and the
list can't drift on "what counts as a species." One source of truth on the backend,
not reimplemented in JS on two pages.

## Section 2 — Lightbox action (everywhere)

Move the panel out of `life_list.html` into the shared lightbox in `_navbar.html`,
driven by the new `life_list` block (not page-local data). Hook the existing
`lightbox:photochanged` event; append to `#lightboxActions` (same insertion point
used today).

Behavior, per the current photo's `life_list` array:

- **0 species** → panel hidden.
- **1 species**, `is_current_photo` false → **"Add to Life List — American Robin"**
  (primary). True → **"★ Life List photo — American Robin"** (selected/inert).
- **2+ species** → one row per species, each with its own button + state.

Click → `POST /api/photo-preferences` (`purpose: 'life_list'`, that species, this
photo) → re-read state → `showToast('Life List photo set for American Robin',
'success')`.

Then delete the duplicated panel code from `life_list.html`; the life-list page
keeps working because it uses the same shared lightbox.

## Section 3 — Browse right-click context menu

Add an entry to `buildPhotoContextMenu(photoIds)` in `browse.html`. The local
`photos` array already carries `species` names per card, so no fetch is needed:

- **Single photo, 0 species** → item hidden.
- **Single photo, 1 species** → **"Add to Life List — American Robin"**.
- **Single photo, 2+ species** → parent **"Add to Life List ▸"** with one child per
  species; if the shared context menu has no submenu support, fall back to one flat
  item per species. Confirm against the renderer during implementation.
- **Multi-selection (2+ photos)** → disabled, hint **"Select a single photo"**
  (matches how "Find Similar" / "View on Map" disable for multi-select).

Fire-and-forget: the menu performs the set and toasts. It does **not** show a ★
"already the photo" state (the local array lacks that fact, and fetching per
right-click would delay menu open). The lightbox remains the surface that shows
current state. Server-side `_photo_can_be_preference` is the backstop against a
stale menu setting an invalid photo/species pair.

## Section 4 — Testing

**Backend** (`vireo/tests/test_photos_api.py` / `test_app.py`):

- one species keyword → one entry, `is_current_photo` false;
- after `POST /api/photo-preferences` → that entry flips to true;
- species keyword in a folder not visible to the active workspace → empty;
- plain non-species keyword → empty;
- two species → two entries with independent `is_current_photo`.

**Regression:** keep `_photo_can_be_preference` validation test green after moving
the panel; it rejects photo/species mismatches so a stale UI can't set an invalid
pair.

**Frontend (manual, real browser):** lightbox button on a non-life-list page;
multi-species shows multiple rows; crowning updates the ★ state and the `/life-list`
grid; right-click single/multi/zero-species behaves; `/life-list` page still works
after its duplicated panel code is removed.

## Out of scope (YAGNI)

- Tagging a photo with a *new* species from these surfaces (that's the identify/
  keyword flow, not this change).
- Unsetting / clearing a representative (no such affordance exists today).
- A ★ current-state indicator inside the browse context menu.
