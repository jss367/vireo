# Compact species confirmations and bulk assignments

Species confirmation supports two explicit response modes. `POST /api/encounters/species`
(and its `/api/v1` alias) defaults to `response_mode: "full"`, preserving the
existing encounter-tree response. Send `response_mode: "compact"` to receive only
changed encounter records. Invalid modes fail before any writes. Preview requests
retain their existing preview contract; the mode selects the final apply response.

A compact confirmation uses the same replace/add/remove, preview-token, burst,
exact-photo-subset and grouping rules as a full confirmation. For example:

```json
{
  "species": "Blue Jay",
  "photo_ids": [101, 102],
  "add": true,
  "response_mode": "compact"
}
```

The response includes `photo_ids`, `changed_photo_ids`, `unchanged_photo_ids`, the
stored `keyword` identity (`id`, canonical `name`, `taxon_id`, `source_taxon_id`,
parent and type), `counts`, `warnings`, `undo`, `cache_changed`, `summary`, and
`encounter_changes`. Existing scalar confirmation fields remain available.
`keyword` is null when a removal has no matching stored target keyword.

Counts describe **keyword changes**: `selected`, `changed` and `unchanged` count
unique photos; `added` and `removed` count photos gaining or losing a keyword.
A replacement can add and remove on the same photo, counting it once in `changed`.
A cache-only confirmation can have zero keyword changes and still have
`cache_changed: true` and a valid undo reference. Low detector confidence produces
structured warnings with the affected photo IDs; the explicit assignment still
applies. Keep/reject flags change only when the existing, separately previewed
Group Review choices explicitly request that change.

`encounter_changes.removed` contains the ordered photo-ID membership of each old
encounter to remove. `upserts` contains `{index, encounter}` records in ascending
final index order. Remove the old memberships first, then insert the upserts at
their final indices. This handles label changes, splits and adjacent merges without
returning unchanged encounters. `base_structure_digest` binds the patch to the
ordered encounter and burst memberships before the edit. The client checks this
digest and the removed memberships before splicing; stale grouping state triggers
an authoritative reload, with no partial patch or summary update. Reload responses
cannot overwrite a newer workspace or scope navigation. No encounter cache produces
an empty patch. Summary counts describe the saved result. The review grid and Group Review request compact
responses and apply these changes locally, preserving unrelated encounter objects.
They do not fetch or POST a complete encounter tree after a species addition when
the grouping still matches. A stale grouping triggers one authoritative fetch.

An undo reference is either null (no edit was recorded) or:

```json
{"edit_id": 123, "endpoint": "/api/undo", "requires_latest": true}
```

POST `{"edit_id": 123}` to the endpoint. It reverses that edit only if it is still
the latest undoable edit in the active workspace; otherwise it returns 409 without
undoing a newer action. An ordinary POST without `edit_id` retains existing LIFO
behavior. Redo uses the existing `/api/redo` endpoint.

## Bulk additive assignment

`POST /api/encounters/species/bulk` (also `/api/v1/encounters/species/bulk`) adds
multiple species to independent target sets in one request:

```json
{
  "response_mode": "compact",
  "assignments": [
    {"species": "Blue Jay", "photo_ids": [101], "detection_ids": [501]},
    {"species": "American Wigeon", "photo_ids": [102, 103]}
  ]
}
```

The versioned route uses the usual `X-Vireo-Token` authentication. A batch accepts
1–100 selections and only compact responses. Each selection requires a species
name and a nonempty list of unique photo IDs. Optional `detection_ids` must be
unique, exist on those exact photos, and include at least one detection for every
submitted photo. They constrain and identify the selected subjects; keywords remain
photo metadata. The operation never expands to sibling photos or accepts/rejects
any classifier predictions. Existing confirmed species and keep/reject flags are
preserved. Replace, remove, splitting and combined flag edits continue to use the
individual confirmation endpoint and its existing preview workflow; unsupported
bulk fields are rejected explicitly.

All selections are validated under the workspace regroup and database writer locks
before writes. Invalid selections report their zero-based assignment index and
apply nothing. Runtime errors roll back every keyword, pending sidecar change and
history record, restoring the prior cache after an attempted write. There is no
partial success. Successful responses declare `atomic: true`, report per-selection
results in submission order, and provide one undo reference for the whole batch.
Repeated or overlapping selections count only actual additions at each step; the
batch's `changed` count is the union of changed photos and its `added` count is the
number of new photo–keyword associations.

The cache is loaded once, affected encounter labels are refreshed after all keyword
writes, and the cache is saved once. One history snapshot covers the whole batch.

## Measured 30-species task

Measured locally on 2026-10-24 with the actual Flask endpoints and temporary SQLite
catalogs. The deterministic synthetic fixture has 530 cataloged photos, one photo
per encounter, and 30 distinct species assigned to 30 selected photos. Every
encounter includes synthetic grouping evidence to represent a large saved review.
These measurements do not rerun the original private wildlife review.

| Work for all 30 species | Full individual responses | Compact individual responses | One bulk request |
| --- | ---: | ---: | ---: |
| HTTP requests | 30 | 30 | 1 |
| Response bytes, uncompressed | 151,583,433 | 316,061 | 24,904 |
| Responses serializing the complete encounter tree | 30 | 0 | 0 |
| Cache loads | 30 | 30 | 1 |
| Cache saves | 30 | 30 | 1 |
| Full before/after history snapshots serialized | 30 | 30 | 1 |
| History snapshot bytes | 306,511,230 | 306,511,230 | 9,940,183 |

Individual compact requests still save the existing cache format and history
snapshots. The bulk endpoint removes that repetition for a batch; it does not
eliminate the one full cache save or undo snapshot. No elapsed-time speedup is
claimed. Reproduce the byte and serialization counters with:

```bash
python -m pytest vireo/tests/test_species_assignment_payloads.py -q -s
node --test tests/frontend/species-assignment-results.test.cjs
```

The client test applies 30 compact responses to a 530-encounter review, verifies
summary updates after each addition, retains all 500 unrelated encounter objects,
and observes exactly 30 confirmation requests with no full-results fetch or cache
save request. Additional cases cover structural changes and Group Review flags.
