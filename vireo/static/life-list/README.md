# Life List page JavaScript

`vireo/templates/life_list.html` (the Life List page at `/life-list`) loads
these classic scripts after the shared navbar scripts and
`vireo-folder-browser.js`. Functions remain global for the page's inline event
handlers (`llSwitchTab`, `debounceRender`, the export, publish and site-export
dialog buttons, and the explorer's class select and taxonomy download button).

| File | Responsibility |
| --- | --- |
| `list.js` | `currentData`, view-preference restore, `escapeAttr`/`formatDate`, and `loadLifeList()` |
| `filters.js` | Taxonomic group and identification-level options, sorting, `debounceRender`, and the list-control listeners |
| `cards.js` | Species cards, paging more photos per species, and `render()` |
| `lightbox.js` | Shared-lightbox listeners: page prefetch, the navigation boundary, Pick changes, and close |
| `export.js` | The Export Life List dialog: formats, CSV columns, and the download link |
| `publish.js` | Website publishing: options, the preflight summary, the shared folder browser, and the publish job |
| `site-export.js` | The Export Entire Site dialog, its folder picker, the job, and its Escape key |
| `explorer.js` | Explorer state (`explorerData`, `explorerPath`, `explorerRankView`), loading a class, the taxonomy download, and the honesty states |
| `explorer-cards.js` | Summary chips, the drill body, breadcrumbs, and taxon cards with progress rings |
| `sunburst.js` | The zoomable sunburst overview and its tooltip |
| `explorer-species.js` | The species leaf for a genus |
| `rank-view.js` | The flat rank breakdown opened from a summary chip |
| `tabs.js` | List/Explorer tab switching and the explorer's lazy first load |
| `boot.js` | Load-time listeners, the folder browser, the first load, and the `?view=` tab |

Load `boot.js` last. Every other file only declares functions and initializes
passive state, so nothing runs across files at load. Boot binds the lightbox
listeners (`bindLifeListLightboxEvents`), the list controls
(`bindLifeListControls`), constructs `publishFolderBrowser`
(`createPublishFolderBrowser`, shared by the publish and site-export pickers),
binds the site-export Escape key (`bindSiteExportEscape`) and the
`lifelist:changed` refresh, then restores view preferences, starts
`loadLifeList()`, and opens the Explorer tab when `?view=explorer`, in the order
the inline script used to run them. Adding a script requires an explicit
template tag before boot. These are not ES modules and need no bundler.

State ownership: `currentData` (`list.js`) is the `/api/life-list` payload,
read by the filters, the cards and the lightbox listeners; per-species paging
state (`lifeListLoadPromises`, `lifeListLightboxSpecies`) lives in `cards.js`;
the explorer's drill state lives in `explorer.js`, with the leaf payload in
`explorer-species.js` and the rank-view request token and search debounce in
`rank-view.js`.

Keep these request-ordering rules intact when changing this code:

- `loadLifeList()` owns `lifeListLoadSeq`. It runs at load, after a Pick
  changes and on `lifelist:changed`; a response for an older sequence must be
  dropped, not rendered.
- `loadMoreLifeListPhotos()` keeps one in-flight page per species in
  `lifeListLoadPromises`. The lightbox prefetch and the navigation-boundary
  listener share that promise, and the boundary only advances if
  `vireoLightboxSession.requestedPhotoId()` is still the photo it started from.
- `refreshPublishPreflight()` owns `publishPreflightSequence`. Any settings
  change or closing the dialog bumps it; a superseded response, or one that
  lands after the dialog closed, is dropped. `publishPreflightReady` keeps
  Publish disabled until the current preflight succeeds.
- The native folder pickers compare `publishFolderBrowser.sequence` before and
  after `pickDirectory()`, and reopen the in-page browser without a fresh
  browse when it moved.
- `siteExportStarting` blocks a second start and keeps the dialog open while
  the job request is in flight.
- `explorerRankReqId` (`rank-view.js`) drops stale `/explorer/rank` responses.
  Loading a class, closing the rank view, going back to cards and opening a
  rank row all bump it. `openRankView()` and going back clear
  `rankSearchTimer`, and its callback ignores a closed rank view.
- `wireSummaryBar()` binds once on the persistent `#tab-explorer` panel;
  `wireBreadcrumb()` binds on each fresh `#explorerBody`. Re-rendering must not
  add per-render listeners to either.
