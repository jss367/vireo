# Keywords page JavaScript

`vireo/templates/keywords.html` (Keywords, at `/keywords`) is a **module
page** (`frontend-module-pages.json`; see `vireo/static/misses/README.md` for
the pattern). After the shared `keywords-link-place-url.js`, the template
loads one entry module:

```html
<script type="module" src="/static/keywords/boot.js"></script>
```

and the browser loads every other file through `boot.js`'s imports.

| File | Responsibility |
| --- | --- |
| `state.js` | Page state: `TYPE_ICONS` / `TYPES`, and the `state` object (`allKeywords` and `identityGroups`, `showRecords`, sort and filter state, `selectedIds`, `parentMap`, the open type dropdown, the merge dialog's state, and the link-to-place flow's Maps key, language preference, loader promise and active autocomplete) |
| `shared.js` | The boundary with shared code: the escape and text-search helpers, `showToast`, `openExternalLink`, `openContextMenu`, `coerceSelectionOnContext`, `openLinkPlaceFromUrl` (`keywords-link-place-url.js`), the loaded Leaflet and Google Maps objects, and `setGoogleMapsCallback` |
| `loading.js` | `loadKeywords()`: the `/api/keywords/identities` fetch that every edit refetches through, pruning the selection to editable rows |
| `filtering.js` | `getFiltered`, the unlinked-location chip count, `isActionableUnlinkedLocation`, and `sortList` |
| `render.js` | `render()`: the table rows, the empty state, the stats line and the unlinked count |
| `filters.js` | The records/groups toggle and "Edit individual records", the search box, the type filter buttons, and column sorting |
| `location-review.js` | The location-match review panel: `loadLocationMatches` and combining a keyword into its linked place |
| `selection.js` | Row and select-all checkboxes, `updateSelectAllCheckbox`, and the bulk bar |
| `merge-state.js` | The merge dialog's elements (its state is in `state.js`) |
| `merge-helpers.js` | `osmLink`, `formatCoords`, `speciesTitle` / `speciesSubtitle`, and the `mergeChoice` radio markup |
| `merge-map.js` | `loadLeaflet` and `renderMergeMap` (every candidate point plus the kept one) |
| `merge-fields.js` | `renderMergeFields` and `wireMergeFields` for the record to keep, the delegated chip and radio handlers, and `syncCoordinateInputs` |
| `merge-summary.js` | `renderMergeSummary`: what the merge will do, in prose |
| `merge-dialog.js` | `scheduleMergePreview`, `refreshMergePreview`, `openMergeDialog`, and the dialog's target, cancel, close and confirm handlers |
| `type-dropdown.js` | Inline type change: the badge-click dropdown, `closeDropdown`, and `updateType` |
| `rename.js` | Inline rename: the double-click editor and `renameKeyword` |
| `bulk.js` | Bulk apply type and `deleteSelectedKeywords` |
| `context-menu.js` | `buildKeywordContextMenu` and the identity-group menu |
| `link-place.js` | The link-to-Google-place flow: `loadGoogleMapsJs`, the `/api/config` fetch (`_cfgPromise`), the modal, `normalizeGooglePlaceForSubmit`, `submitLinkPlace`, and the per-row Link button and modal close controls |
| `boot.js` | The entry module: binding every listener in the old inline script's order, then the first `loadKeywords()` and the `?link_place=` deep link |

Page state is the one `state` object in `state.js`; files write its fields,
since a module cannot reassign another module's binding. The page registers
no page hooks. Its one `window` write (`sharedWrites`) is the Google Maps
callback, `_gmapsCallback`, which the Maps script calls by the name the
loader passes in its URL (`&callback=_gmapsCallback`); Browse sets the same
name for its own loader. The Maps API key and language preference that used
to be `window.GOOGLE_MAPS_*` globals are `state.googleMapsApiKey` and
`state.googleMapsPreferEnglish`, and the Leaflet loader's promise is local to
`merge-map.js`.

Module scripts run after the document is parsed, so `merge-state.js` can read
the dialog's elements when it is evaluated, and `link-place.js` starts the
`/api/config` fetch that `openLinkPlaceModal` awaits. The listeners the old
inline script attached at load are wrapped in `bind*()` functions
(`bindKeywordRecordToggles`, `bindLocationReview`, `bindKeywordSearch`,
`bindKeywordFilterButtons`, `bindKeywordSorting`, `bindKeywordSelection`,
`bindMergeFieldDelegates`, `bindMergeDialog`, `bindKeywordTypeBadges`,
`bindKeywordRename`, `bindKeywordBulkApply`, `bindKeywordContextMenu`,
`bindLinkPlaceControls`); boot calls them in the original order, with the
one-line registrations in their places between them: the search options'
match-case and whole-word buttons (which used to carry inline `onclick`s),
the merge dialog's OpenStreetMap links (`a[data-external-link]`, routed
through `openExternalLink`), `closeDropdown` on document click, and
`deleteSelectedKeywords` on the bulk delete button. `#kwBody` has three click
listeners (edit-records, type badge, Link button) whose order boot preserves.

Browser tests reach the modules through `tests/e2e/module_pages.py`
(`install_page_module_exposure(page, "keywords", "keywordsState")`): they read
`keywordsState.selectedIds` and call `pageModules.linkPlace.submitLinkPlace`,
which used to be the `window.__kwSelectedIds` and `window.__linkPlaceForTest`
test hooks.

Keep these guards intact when changing this code:

- `refreshMergePreview()` stamps each request with `++state.mergeRequest` and drops
  a response for an older request or a closed dialog; the dialog's `close`
  handler bumps the counter and clears the debounce timer so a late preview
  cannot repopulate a closed dialog.
- `state.mergeToken` is the server's answer that the preview is complete; Confirm
  stays disabled without one (the preview is still asking a question, such as
  two different linked places). `state.mergeSaving` blocks Escape while the merge
  request is in flight.
- `state.mergeOverrides` holds only the fields the user picked; an absent key means
  "use the kept record's value", which the server also falls back to. Both
  `state.mergeOverrides` and `state.mergeFieldsFor` reset when the record to keep changes,
  so one record's picks never apply to another.
- The fields are rebuilt only when `state.mergeFieldsFor !== targetId`; re-rendering
  on every preview would fight typing in the name and coordinate inputs.
  `syncCoordinateInputs` is skipped once `state.mergeOverrides.coordinates` is set
  for the same reason.
- `bindMergeFieldDelegates()` registers the chip and radio handlers on the
  persistent `#kwMergeFields` container once; `wireMergeFields` only binds the
  per-control inputs that the next render discards.
- Every edit (type, rename, bulk type, delete, merge, link) refetches through
  `loadKeywords()` because the server may answer `merged`, deleting the
  requested id in favour of a normalized-equal peer.
- `openLinkPlaceModal` awaits `_cfgPromise` before deciding between
  autocomplete and the missing-key error; `loadGoogleMapsJs` memoizes its
  promise (including a rejection) so the Maps script is requested once.
