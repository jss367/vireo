# Keywords page JavaScript

`vireo/templates/keywords.html` (Keywords, at `/keywords`) loads these classic
scripts after the shared navbar scripts and `keywords-link-place-url.js`,
where its inline page script used to be. The inline script was one IIFE; its
state and functions are now page globals shared across these files. The page
exposes two test hooks by name (`window.__kwSelectedIds`,
`window.__linkPlaceForTest`) and relies on the navbar's `escapeHtml`,
`escapeAttr`, `showToast`, `openContextMenu`, `coerceSelectionOnContext` and
`openExternalLink`, `VireoTextSearch` from `vireo-utils.js`, and
`openLinkPlaceFromUrl` from `keywords-link-place-url.js`.

| File | Responsibility |
| --- | --- |
| `state.js` | Page state: `TYPE_ICONS` / `TYPES`, `allKeywords` and `identityGroups`, `showRecords`, sort and filter state, `selectedIds`, `parentMap`, and the open type dropdown |
| `loading.js` | `loadKeywords()`: the `/api/keywords/identities` fetch that every edit refetches through, pruning the selection to editable rows |
| `filtering.js` | `getFiltered`, the unlinked-location chip count, `isActionableUnlinkedLocation`, and `sortList` |
| `render.js` | `render()`: the table rows, the empty state, the stats line and the unlinked count |
| `filters.js` | The records/groups toggle and "Edit individual records", the search box, the type filter buttons, and column sorting |
| `location-review.js` | The location-match review panel: `loadLocationMatches` and combining a keyword into its linked place |
| `selection.js` | Row and select-all checkboxes, `updateSelectAllCheckbox`, and the bulk bar |
| `merge-state.js` | The merge dialog's elements and state: `mergeIds`, `mergeToken`, `mergeRequest`, `mergeSaving`, `mergeOverrides`, `mergeFieldsFor`, `mergeDebounce`, the Leaflet map and layer, and `mergeFieldOptions` |
| `merge-helpers.js` | `osmLink`, `formatCoords`, `speciesTitle` / `speciesSubtitle`, and the `mergeChoice` radio markup |
| `merge-map.js` | `loadLeaflet` and `renderMergeMap` (every candidate point plus the kept one) |
| `merge-fields.js` | `renderMergeFields` and `wireMergeFields` for the record to keep, the delegated chip and radio handlers, and `syncCoordinateInputs` |
| `merge-summary.js` | `renderMergeSummary`: what the merge will do, in prose |
| `merge-dialog.js` | `scheduleMergePreview`, `refreshMergePreview`, `openMergeDialog`, and the dialog's target, cancel, close and confirm handlers |
| `type-dropdown.js` | Inline type change: the badge-click dropdown, `closeDropdown`, and `updateType` |
| `rename.js` | Inline rename: the double-click editor and `renameKeyword` |
| `bulk.js` | Bulk apply type and `deleteSelectedKeywords` |
| `context-menu.js` | `buildKeywordContextMenu`, the identity-group menu, and the `window.__kwSelectedIds` test hook |
| `link-place.js` | The link-to-Google-place flow: `loadGoogleMapsJs`, the `/api/config` fetch (`_cfgPromise`), the modal, `normalizeGooglePlaceForSubmit`, `submitLinkPlace`, the per-row Link button and modal close controls, and the `window.__linkPlaceForTest` hook |
| `boot.js` | Binding every listener in the inline script's order, then the first `loadKeywords()` and the `?link_place=` deep link |

Load `boot.js` last. Three files do more than declare at load time, so their
template order matters: `merge-state.js` reads the dialog's elements,
`context-menu.js` assigns `window.__kwSelectedIds = selectedIds` (so
`state.js` must precede it), and `link-place.js` starts the `/api/config`
fetch that `openLinkPlaceModal` awaits. The listeners the inline script
attached at load are wrapped in `bind*()` functions (`bindKeywordRecordToggles`,
`bindLocationReview`, `bindKeywordSearch`, `bindKeywordFilterButtons`,
`bindKeywordSorting`, `bindKeywordSelection`, `bindMergeFieldDelegates`,
`bindMergeDialog`, `bindKeywordTypeBadges`, `bindKeywordRename`,
`bindKeywordBulkApply`, `bindKeywordContextMenu`, `bindLinkPlaceControls`);
boot calls them in the original order, with the two one-line registrations
(`closeDropdown` on document click, `deleteSelectedKeywords` on the bulk
delete button) in their original places between them. `#kwBody` has three
click listeners (edit-records, type badge, Link button) whose order boot
preserves. Adding a script requires an explicit template tag before boot.
These are not ES modules and need no bundler.

Keep these guards intact when changing this code:

- `refreshMergePreview()` stamps each request with `++mergeRequest` and drops
  a response for an older request or a closed dialog; the dialog's `close`
  handler bumps the counter and clears the debounce timer so a late preview
  cannot repopulate a closed dialog.
- `mergeToken` is the server's answer that the preview is complete; Confirm
  stays disabled without one (the preview is still asking a question, such as
  two different linked places). `mergeSaving` blocks Escape while the merge
  request is in flight.
- `mergeOverrides` holds only the fields the user picked; an absent key means
  "use the kept record's value", which the server also falls back to. Both
  `mergeOverrides` and `mergeFieldsFor` reset when the record to keep changes,
  so one record's picks never apply to another.
- The fields are rebuilt only when `mergeFieldsFor !== targetId`; re-rendering
  on every preview would fight typing in the name and coordinate inputs.
  `syncCoordinateInputs` is skipped once `mergeOverrides.coordinates` is set
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
