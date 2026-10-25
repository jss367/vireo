// Merge dialog state: the dialog's elements, the ids being merged, the preview
// token and request counter, per-field overrides, and the Leaflet map.

// -- Explicit merge: pick a record to keep, then compose the result --
export const mergeDialog = /** @type {HTMLDialogElement} */ (document.getElementById('kwMergeDialog'));
export const mergeTarget = /** @type {HTMLSelectElement} */ (document.getElementById('kwMergeTarget'));
export const mergeConfirm = /** @type {HTMLButtonElement} */ (document.getElementById('kwMergeConfirm'));
export const mergeCancel = /** @type {HTMLButtonElement} */ (document.getElementById('kwMergeCancel'));
export const mergeError = /** @type {HTMLElement} */ (document.getElementById('kwMergeError'));
export const mergePreview = /** @type {HTMLElement} */ (document.getElementById('kwMergePreview'));
export const mergeFields = /** @type {HTMLElement} */ (document.getElementById('kwMergeFields'));
export const mergeMapEl = /** @type {HTMLElement} */ (document.getElementById('kwMergeMap'));
export const mergeMapNote = /** @type {HTMLElement} */ (document.getElementById('kwMergeMapNote'));
// The merge dialog's own state (the ids, preview token and request counter,
// per-field overrides and the map) is in state.js's `state`.
