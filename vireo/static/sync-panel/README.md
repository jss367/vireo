# Sync panel JavaScript

`vireo/templates/_sync_panel.html` is a partial: the pending-XMP-changes
banner and the Review Changes dialog, included by Browse, Review, Location
Review and Pipeline Review. It loads classic scripts from
`vireo/static/sync-panel/` by responsibility, deferred, with `boot.js` last.
The partial sits before each including page's own scripts, so the functions
those pages call (`checkPendingSync`, `runSync`) exist by the time they run.
Browser tests read and set the panel's globals (`_syncModalOpen`,
`_syncPreviewData`, ...) and call `openSyncPreview` directly.

| File | Responsibility |
| --- | --- |
| `state.js` | Shared state: the preview and its load (token, abort controller, timers, display limit, completeness), the change-type filters, the dialog and lightbox-panel state, and the running sync job |
| `status.js` | The banner: progress text, the running job, and polling `/api/sync/status` (`checkPendingSync`) |
| `run.js` | `runSync` and `_syncShowSyncResult`, which reports what actually reached the sidecars |
| `preview-load.js` | The dialog's thumbnail size, progress line and action buttons, and the progressive load of `/api/sync/preview` (restarted once when a sync starts or the queue changes) |
| `preview-render.js` | Change-type labels and styles, grouping changes, and rendering the filters, summary, groups and photos |
| `preview-actions.js` | Closing the dialog, Sync to XMP for the checked types, discards, and the type filters |
| `lightbox-panel.js` | The pending-changes panel beside the lightbox and its per-photo discards |
| `boot.js` | Registers the `runSync` page hook (the native menu's Sync Metadata; was `sync-panel-page-hooks.js`), binds the controls that used to carry inline handlers, the dialog's thumbnail clicks and the lightbox listeners |

Markup rendered by `preview-render.js` still carries inline handlers that
call these globals.

## Guards

- A sync that ends without counts is reported as a failure or a cancellation,
  never as "Wrote 0"; a cancelled run that already saw failures reports them.
- `_syncPreviewComplete`, not `_syncPreviewLoading`, enables Sync and
  Discard, so a partial load never offers actions on a truncated queue.
- `_syncPreviewLoadToken` and the abort controller drop responses from a
  superseded load.
