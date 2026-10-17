// Merge dialog state: the dialog's elements, the ids being merged, the preview
// token and request counter, per-field overrides, and the Leaflet map.
// Classic page script; load boot.js after all definitions.

// -- Explicit merge: pick a record to keep, then compose the result --
const mergeDialog = /** @type {HTMLDialogElement} */ (document.getElementById('kwMergeDialog'));
const mergeTarget = /** @type {HTMLSelectElement} */ (document.getElementById('kwMergeTarget'));
const mergeConfirm = /** @type {HTMLButtonElement} */ (document.getElementById('kwMergeConfirm'));
const mergeCancel = /** @type {HTMLButtonElement} */ (document.getElementById('kwMergeCancel'));
const mergeError = /** @type {HTMLElement} */ (document.getElementById('kwMergeError'));
const mergePreview = /** @type {HTMLElement} */ (document.getElementById('kwMergePreview'));
const mergeFields = /** @type {HTMLElement} */ (document.getElementById('kwMergeFields'));
const mergeMapEl = /** @type {HTMLElement} */ (document.getElementById('kwMergeMap'));
const mergeMapNote = /** @type {HTMLElement} */ (document.getElementById('kwMergeMapNote'));
/** @type {number[]} */ let mergeIds = [];
/** @type {string|null} */ let mergeToken = null;
let mergeRequest = 0;
let mergeSaving = false;
// The dialog's per-field picks. Absent key = "use the kept record's value",
// which is what the server falls back to, so an untouched field keeps
// tracking the record-to-keep dropdown instead of freezing on first render.
/** @type {KeywordMergeOverrides} */ let mergeOverrides = {};
/** @type {number|null} */ let mergeFieldsFor = null;
/** @type {ReturnType<typeof setTimeout>|null} */ let mergeDebounce = null;
/** @type {KeywordLeafletMap|null} */ let mergeMap = null;
/** @type {KeywordLeafletLayer|null} */ let mergeMapLayer = null;
// Latest option set, read by the once-registered delegated handlers.
/** @type {KeywordMergeOptions|null} */ let mergeFieldOptions = null;
