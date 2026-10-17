// Merge dialog state: the dialog's elements, the ids being merged, the preview
// token and request counter, per-field overrides, and the Leaflet map.
// Classic page script; load boot.js after all definitions.

// -- Explicit merge: pick a record to keep, then compose the result --
const mergeDialog = document.getElementById('kwMergeDialog');
const mergeTarget = document.getElementById('kwMergeTarget');
const mergeConfirm = document.getElementById('kwMergeConfirm');
const mergeCancel = document.getElementById('kwMergeCancel');
const mergeError = document.getElementById('kwMergeError');
const mergePreview = document.getElementById('kwMergePreview');
const mergeFields = document.getElementById('kwMergeFields');
const mergeMapEl = document.getElementById('kwMergeMap');
const mergeMapNote = document.getElementById('kwMergeMapNote');
let mergeIds = [];
let mergeToken = null;
let mergeRequest = 0;
let mergeSaving = false;
// The dialog's per-field picks. Absent key = "use the kept record's value",
// which is what the server falls back to, so an untouched field keeps
// tracking the record-to-keep dropdown instead of freezing on first render.
let mergeOverrides = {};
let mergeFieldsFor = null;
let mergeDebounce = null;
let mergeMap = null;
let mergeMapLayer = null;
// Latest option set, read by the once-registered delegated handlers.
let mergeFieldOptions = null;
