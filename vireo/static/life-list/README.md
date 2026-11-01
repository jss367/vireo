# Life List page JavaScript

`vireo/templates/life_list.html` (the Life List page at `/life-list`) is a
**module page** (`frontend-module-pages.json`; see
`vireo/static/misses/README.md` for the pattern). After the shared
`vireo-folder-browser.js`, the template loads one entry module:

```html
<script type="module" src="/static/life-list/boot.js"></script>
```

and the browser loads every other file through `boot.js`'s imports.

| File | Responsibility |
| --- | --- |
| `state.js` | Page state: the `state` object (`currentData`, its load sequence, per-species paging, the export column picks, the publish preflight, site export, the shared folder browser, and the explorer's drill, rank-view, sunburst and leaf state) and `publishFolderBrowser()` |
| `shared.js` | The boundary with shared code: the escape, view-preference and text-search helpers, `safeFetch`, `safeEventSource`, `showToast`, the lightbox (`openLightbox`, the requested photo), the thumbnail URL, the desktop app's folder picker and `isTauri`, the folder browser constructor, and `registerPageHooks()` |
| `list.js` | View-preference restore, `formatDate`, and `loadLifeList()` |
| `filters.js` | Taxonomic group and identification-level options, sorting, `debounceRender`, and the list-control listeners |
| `cards.js` | Species cards, paging more photos per species, and `render()` |
| `lightbox.js` | Shared-lightbox listeners: page prefetch, the navigation boundary, Pick changes, and close |
| `export.js` | The Export Life List dialog: formats, CSV columns, and the download link |
| `publish.js` | Website publishing: options, the preflight summary, the shared folder browser, and the publish job |
| `site-export.js` | The Export Entire Site dialog, its folder picker, the job, and its Escape key |
| `explorer.js` | Loading a class, the taxonomy download, and the honesty states |
| `explorer-cards.js` | Summary chips, the drill body, breadcrumbs, and taxon cards with progress rings |
| `sunburst.js` | The zoomable sunburst overview and its tooltip |
| `explorer-species.js` | The species leaf for a genus |
| `rank-view.js` | The flat rank breakdown opened from a summary chip |
| `tabs.js` | List/Explorer tab switching and the explorer's lazy first load |
| `events.js` | The template's `data-life-on` bindings, and the explorer panel's delegated listeners for the rendered taxonomy-download button and class select |
| `boot.js` | The entry module: the markup bindings, load-time listeners, the folder browser, the page hooks, the first load, and the `?view=` tab |

Page state is the one `state` object in `state.js`; files write its fields,
since a module cannot reassign another module's binding. The page writes no
`window` globals. It registers two page hooks: `afterHistoryChange` (the
navbar's undo/redo reloads the list) and `openExportModal` (the native menu's
Export opens the Life List export).

The template carries no inline `on*=` attributes. A control names its handler
in `data-life-on="event:name"` (with `data-life-arg` for `llSwitchTab`'s tab);
`events.js` binds each from its `LIFE_HANDLERS` table, passing the event and
the element, so the export and site-export overlays' backdrop closes (`if
(event.target === el)`) behave as before; an unknown name throws. Boot binds
those first, then, in the old order, the lightbox listeners
(`bindLifeListLightboxEvents`), the list controls (`bindLifeListControls`),
the shared folder browser (`state.publishFolderBrowser =
createPublishFolderBrowser()`, used by the publish and site-export pickers
through `publishFolderBrowser()`), the site-export Escape key
(`bindSiteExportEscape`), the `lifelist:changed` refresh and the page hooks,
then restores view preferences, starts `loadLifeList()`, and opens the
Explorer tab when `?view=explorer`.

Browser tests reach the modules through `tests/e2e/module_pages.py`
(`install_page_module_exposure(page, "life-list", "lifeListState")`), e.g.
`lifeListState.currentData`, `lifeListState.lifeListLightboxSpecies`, and
`lifeListState.lifeListLightboxTaxonId`.

Keep these request-ordering rules intact when changing this code:

- `loadLifeList()` owns `state.lifeListLoadSeq`. It runs at load, after a Pick
  changes and on `lifelist:changed`; a response for an older sequence must be
  dropped, not rendered.
- `loadMoreLifeListPhotos()` keeps one in-flight page per taxon (or unlinked name) in
  `state.lifeListLoadPromises`. Cards and paging requests carry `taxon_id` so
  distinct taxa sharing a name stay separate; `taxon_id=none` explicitly pages
  the unlinked entry, while omitting the parameter keeps legacy name-only loads. The lightbox prefetch and the navigation-boundary
  listener share that promise, and the boundary only advances if
  `vireoLightboxSession.requestedPhotoId()` is still the photo it started from.
- `refreshPublishPreflight()` owns `state.publishPreflightSequence`. Any settings
  change or closing the dialog bumps it; a superseded response, or one that
  lands after the dialog closed, is dropped. `state.publishPreflightReady` keeps
  Publish disabled until the current preflight succeeds.
- The native folder pickers compare `publishFolderBrowser().sequence` before and
  after `pickDirectory()`, and reopen the in-page browser without a fresh
  browse when it moved.
- `state.siteExportStarting` blocks a second start and keeps the dialog open while
  the job request is in flight.
- `state.explorerViewReqId` drops stale responses for the requests
  that fill `#explorerBody`: `loadExplorerSpecies()` (`/explorer/species`, a
  genus's leaf) and `openRankView()` (`/explorer/rank`) each claim it, and a
  response, or failure, for an older value is discarded. Every navigation that
  replaces the body bumps it: loading a class, opening or closing a rank view,
  going back to cards, opening a rank row, a breadcrumb, drilling a card, and a
  sunburst arc or center click. A new navigation must bump it too. Switching
  tabs does not: the explorer view stays where it was, so its pending request
  still belongs to it. `loadExplorerSpecies()` clears `state.explorerLeafData` when it
  starts, so returning to a genus whose request was superseded reloads it
  rather than showing the previous genus's species. `openRankView()` and going
  back clear `state.rankSearchTimer`, and its callback ignores a closed rank view.
- `wireSummaryBar()` binds once on the persistent `#tab-explorer` panel;
  `wireBreadcrumb()` binds on each fresh `#explorerBody`. Re-rendering must not
  add per-render listeners to either.
