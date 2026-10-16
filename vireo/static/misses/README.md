# Misses page JavaScript

`vireo/templates/misses.html` (Misses, at `/misses`) loads these classic
scripts after the shared navbar scripts and the shared filter bar, where its
inline page script used to be. Functions remain global for the card markup's
`onclick` / `ondblclick` attributes, the selection bar's buttons, and the hooks
the shared lightbox and navbar call by name (`window.afterHistoryChange`,
`window.handleMissesLightboxFlagShortcut`, `openBrowseShortcutPhoto`).

| File | Responsibility |
| --- | --- |
| `state.js` | Page state: category order and labels, `missesData`, the tuning config and preview state, focus, `selection`, lightbox tracking, `appliedMissFilters`, and the fail-closed scope error |
| `helpers.js` | `photoThumbnailUrl`, `parseDetectionBox`, `sortPhotos`, and `getSinceParam` |
| `filters.js` | Reading, cloning and comparing the filter-bar payload, adding it to request bodies, and `applyMissFilters()` |
| `scope.js` | Legacy `?collection_id=` / `?folder_id=` deep links, `noteLegacyScopeError`, and the scope banner |
| `tuning.js` | The threshold sliders and labels, tuning status and disabled state, `loadMissConfig`, and preview, recompute and reset |
| `loading.js` | `applyMissesPayload`, `loadMisses()` with its sequence guard, and the navbar's `afterHistoryChange` hook |
| `render.js` | `render()`, each category section and its cards, and section collapse |
| `photo-open.js` | Opening a miss in the lightbox or Browse, the focused/selected photo lookups, and `pruneSelectionToVisible` |
| `flagging.js` | Bulk reject per category, per-card and selection flag changes, per-card and lightbox unflag, and the Representative badge clearing they share |
| `selection.js` | Clearing and toggling the selection, the selection bar, shift-click ranges, and card click and double-click |
| `context-menu.js` | The right-click menu for miss cards |
| `delete.js` | Deleting the selected photos through the shared delete dialog |
| `keyboard.js` | `setFocused`, `moveFocus`, the keydown handler, and Shift+J/K selection extension |
| `badges.js` | Keeping Representative badges and the applied flag filter current after `lifelist:changed` and `lightbox:flagchanged` |
| `boot.js` | Binding the listeners, the slider handlers, `VireoFilter.init()`, the legacy scope resolution, the config load, and the first load |

Load `boot.js` last. The other files only declare functions and initialize
passive state, so their relative order does not matter at load time. The six
listeners the inline script attached at load are wrapped in
`bindMissLightboxTracking()` (`photo-open.js`), `bindMissContextMenu()`
(`context-menu.js`), `bindMissKeyboard()` (`keyboard.js`),
`bindMissLifeListEvents()` and `bindMissLightboxFlagEvents()` (`badges.js`).
Boot calls them in that order, then runs `installMissSliderHandlers()` and the
`VireoFilter.init()` chain as the inline script did. Adding a script requires
an explicit template tag before boot. These are not ES modules and need no
bundler.

Keep these guards intact when changing this code:

- `loadMisses()` and `previewMisses()` share `missLoadRequestId`; a response
  for an older request is dropped. `previewRequestId` does the same for
  debounced slider previews, and `invalidateMissPreview()` bumps it.
- `appliedMissFilters` is the filter snapshot that produced the cards on
  screen. Recompute and bulk reject send that snapshot, not the live filter
  bar, so they act on the photos the user is looking at.
- `legacyScopeError` / `legacyScopeRecoveryPending` fail closed: an
  unresolvable deep link renders an empty grid and freezes the tuning
  controls (`MISS_TUNING_CONTROL_IDS`) until a replacement filter has loaded.
  Preview, recompute and reset all return early while either is set.
- `previewActive` disables bulk reject, per-card unflag and the selection
  bar's unflag button; the preview endpoint does not persist flags.
- `pruneSelectionToVisible()` runs after every synchronous mutation of
  `missesData` so a follow-up bulk flag cannot act on cards no longer shown.
