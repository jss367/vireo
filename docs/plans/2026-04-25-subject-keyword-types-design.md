# Subject Keyword Types Design

**Date**: 2026-04-25
**Status**: Design

## Problem

Vireo's "Needs Classification" smart collection filters for photos with no species keyword (`has_species == 0`). This conflates two distinct cases:

1. Wildlife photos the user hasn't classified yet.
2. Photos that aren't wildlife at all (landscapes, portraits, places).

There's no way for the user to say "this is a landscape, stop showing it as needing classification, and stop running the classifier on it." The closest existing mechanisms (`flag=rejected`, `prediction_review.status=rejected`, `box_count=0`) are either overloaded with other meanings or don't affect the queue / classifier.

## Solution overview

Generalize the existing `keywords.type` column from a partly-used field (`taxonomy` and `general` in practice) into a fixed five-value enum representing what *kind of thing* a keyword identifies. Define a per-workspace configurable subset of these types as "subject types" — having a keyword of a subject type means the photo is identified.

The "Needs Classification" queue and the classifier both consult this set: an identified photo drops out of the queue and the classifier skips it (with a `reclassify=True` bypass for explicit re-verification).

## 1. Data model

No new tables. No new columns. Reuse the existing `keywords.type` column (`db.py:180`).

**Fixed enum values**:

```python
KEYWORD_TYPES = {'taxonomy', 'individual', 'place', 'scene', 'general'}
SUBJECT_TYPES_DEFAULT = {'taxonomy', 'individual', 'scene'}
```

| Type | Meaning | Examples |
|------|---------|----------|
| `taxonomy` | A species (links to `taxa` via `taxon_id`) | Canada Goose, House Sparrow |
| `individual` | A named person or pet | Charlie, Fido, John |
| `place` | A named location (uses `latitude`/`longitude`) | Golden Gate Bridge, my backyard |
| `scene` | A non-wildlife category | Landscape, Sunset, Architecture |
| `general` | Catch-all untyped tags | needs_review, client_A, portfolio |

**Existing columns repurposed**:

- `keywords.is_species` (`db.py:179`) — kept as a redundant fast-path flag, maintained in sync with `type='taxonomy'` via existing `mark_species_keywords` (`db.py:5652`).
- `keywords.latitude`, `keywords.longitude` (`db.py:181-182`) — already exist; meaningful when `type='place'`.
- `keywords.taxon_id` (`db.py:183`) — meaningful only when `type='taxonomy'`.
- `keywords.parent_id` — taxonomy hierarchy; unused for non-taxonomy types.

**Validation**: application-level (in `add_keyword` and any API endpoint accepting `type`). No DB CHECK constraint — consistent with existing patterns and avoids future migration friction.

**Existing data**: 366 `taxonomy` and 255 `general` rows stay valid as-is. No backfill.

**Explicit non-goals**:
- No `species_keyword_id` FK on individuals (YAGNI; individuals are people/pets, not named wildlife).
- No DB CHECK constraint on `type`.

## 2. Workspace config

Per-workspace override stored in the existing `workspaces.config_overrides` JSON column (`db.py:196`). New top-level key:

```json
{
  "subject_types": ["taxonomy", "individual", "scene"]
}
```

**Default** is set in `vireo/config.py` at the global config level. Workspace overrides only need to write the key when the user actively changes it from the default.

**Read path**: new helper `db.get_subject_types() -> set[str]` derives from `get_effective_config(cfg.load())`. Used by:
- The `has_subject` collection rule
- The classifier's skip check
- Any future "is this photo identified?" predicate

**Validation on write**: incoming list intersected with `KEYWORD_TYPES` — unknown values silently dropped. Empty list allowed (effectively disables the queue's filter) but logs a warning.

**API**: new `PUT /api/workspaces/<id>/subject-types`, body `{"types": [...]}`. Reads via the existing workspace config GET endpoint.

## 3. The `has_subject` collection rule

New rule operator added to the rules engine in `db.py:5414-5557`:

```json
{"field": "has_subject", "op": "equals", "value": 0}
```

Generates `NOT EXISTS` (`==0`) or `EXISTS` (`==1`) against `photo_keywords` joined to `keywords` filtered by the workspace's `subject_types`:

```sql
NOT EXISTS (
  SELECT 1 FROM photo_keywords pk
  JOIN keywords k ON k.id = pk.keyword_id
  WHERE pk.photo_id = p.id AND k.type IN (?, ?, ?)
)
```

The `IN` placeholders are bound at query-build time from `db.get_subject_types()`. Empty `subject_types` short-circuits to a no-op (matches all photos for `==0`, no photos for `==1`) — handled in the rule builder.

**`has_species` is kept** as a narrower predicate. Users can still build queues like "photos already tagged Charlie but with no species confirmed."

**Default collection migration**: the auto-created "Needs Classification" collection (`db.py:5710`) is renamed to **"Needs Identification"** and its rule changes from `has_species == 0` to `has_subject == 0`.

## 4. Classifier integration

Two skip layers in `classify_job.py`, both bypassed when `reclassify=True`:

1. **Existing**: skip photos with prior `classifier_runs` rows (`classify_job.py:248-271`).
2. **New**: skip photos with any keyword whose `type` is in the workspace's `subject_types`.

```python
if not reclassify:
    subject_types = thread_db.get_subject_types()
    if subject_types:
        photo_ids = thread_db.filter_out_subject_tagged(photo_ids, subject_types)
```

`filter_out_subject_tagged()` is a new method on `Database`. Implemented as a single `WHERE id NOT IN (SELECT pk.photo_id FROM photo_keywords pk JOIN keywords k ON k.id = pk.keyword_id WHERE k.type IN (?, ?, ?))` query.

Skip happens *before* detection runs — MegaDetector also benefits from the gate.

**Verify mode**: the existing `reclassify=True` flag's semantics extend to bypass *both* the classifier_runs skip *and* the new subject-tag skip. The "Re-classify" toggle in `pipeline.html` keeps its label; tooltip is updated.

**Job logging**: progress events include skip counts (`{skipped_subject: N, skipped_already_classified: M}`) so the bottom panel shows why fewer photos were processed than the input collection size.

## 5. UI changes

**Keyword creation** gets a type picker. New `<select>` defaulting to `general` next to the keyword name input on the keywords page (`keywords.html`) and inline tag inputs in the lightbox. `taxonomy` is disabled in the inline picker (taxonomy keywords are created by the classifier or `mark_species_keywords`, not free text).

**Keywords page filter buttons** (`keywords.html:162-164`) updated from `general | taxonomy | location` to `general | taxonomy | individual | place | scene`. Existing list view shows `type` per keyword (`db.py:3319`); add a dropdown to edit it on existing rows (except `taxonomy`, locked once linked to `taxon_id`).

**Default scene keywords** shipped via `Database.__init__`: `Landscape`, `Sunset`, `Architecture`, `Abstract`. None for `place` or `individual`. Makes the lightbox autocomplete path work for the common "tag a landscape" case.

**Lightbox quick-tag affordance**: a "Not Wildlife" button next to the existing species accept/reject controls. Clicking it tags the photo with `Landscape` (the default `scene` keyword) and advances. One click per photo to clear from the queue.

**Workspace settings panel**: small "What counts as identified?" section with checkboxes for the five types. Defaults to `taxonomy`, `individual`, `scene`. Saves via the new endpoint.

**Collections panel**: "Needs Classification" → "Needs Identification" in the default collection list.

**Pipeline page**: "Re-classify" toggle keeps its label; tooltip updated to mention it bypasses the new subject-tag skip.

## 6. Migration & idempotence

Three idempotent ops in `Database.__init__`:

1. **Rename the default collection.** If a workspace has a "Needs Classification" collection with rule `has_species == 0` and no "Needs Identification" collection, rename and update the rule. Skip if the user customized either field.
2. **Insert default `scene` keywords** if none exist with `type='scene'`: `Landscape`, `Sunset`, `Architecture`, `Abstract`.
3. **No backfill** of existing keywords.

UI cleanup: rename `data-type="location"` filter button (`keywords.html:164`) to `data-type="place"`.

## 7. Edge cases & testing

**Edge cases**:
- Empty `subject_types` set → `has_subject` rule short-circuits; no malformed SQL.
- Photo with both `taxonomy` and `general` keywords → counted as identified (any matching type wins).
- Workspace switch mid-classify-job → job uses the workspace it was started under (existing pattern via `thread_db.set_active_workspace`).
- User edits a keyword's `type` from `taxonomy` → blocked once linked to `taxon_id`.
- Reclassify-mode on a `scene=Landscape` photo → runs detection + classification, leaves the keyword in place.

**Test coverage**:
- `test_db.py`: `has_subject` rule SQL generation across configurations; `filter_out_subject_tagged` correctness; default-collection migration idempotence; default scene keyword insertion idempotence.
- `test_workspaces.py`: `subject_types` config read/write; isolation across workspaces.
- `test_jobs_api.py`: classify job skips subject-tagged photos by default; `reclassify=True` bypasses; skip counts in progress events.
- `test_app.py`: `PUT /api/workspaces/<id>/subject-types` validation (unknown types dropped, empty allowed-with-warning).

## Out of scope

- Linking individuals to species (e.g., Charlie → Great Blue Heron). YAGNI — individuals here are people/pets.
- Auto-detecting scene type from image content (could ship a "scene classifier" later if useful).
- Per-photo "skip forever" boolean — the keyword tag is the mechanism.
- Editing collection rules through the UI — done via API or by recreating the default collection.
