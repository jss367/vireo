// Sync panel state shared by its classic scripts: the pending-change preview
// and its load (token, abort controller, timers, display limit), the change
// type filters, the modal and lightbox-panel state, and the sync job.
// Classic script; loads first among the sync panel scripts.

/* ---------- Sync Panel ---------- */

/** @type {SyncPreviewData | null} */
var _syncPreviewData = null;
var _syncThumbSize = 120;
/** @type {Set<string> | null} */
var _syncActiveFilters = null;
var _syncModalOpen = false;
/** @type {number | null} */
var _syncEscToken = null;
/** @type {HTMLElement | null} */
var _syncLightboxPanelEl = null;
var _syncPreviewLoading = false;
// Distinct "the load finished cleanly" flag. `_syncPreviewLoading` also
// goes false on partial-load failure, so re-renders (filter toggle, Show
// More) would otherwise re-enable Sync / Discard on a truncated queue.
var _syncPreviewComplete = false;
var _syncPreviewLoadToken = 0;
/** @type {AbortController | null} */
var _syncPreviewAbortController = null;
var _syncPreviewStartedAt = 0;
/** @type {ReturnType<typeof setInterval> | null} */
var _syncPreviewElapsedTimer = null;
var _syncPreviewDisplayLimit = 300;
/** @type {SyncJob | null} */
var _syncActiveJob = null;
var _syncReviewWaiting = false;
/** @type {ReturnType<typeof setTimeout> | null} */
var _syncStatusTimer = null;
/** @type {Promise<SyncStatusSummary | undefined> | null} */
var _syncStatusRequest = null;
