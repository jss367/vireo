// Review page state: the loaded predictions and the active view selections.
// Classic page script; load boot.js after all definitions.

/* ---------- State ---------- */
var predictions = [];
var allPredictions = [];  // unfiltered copy
var currentTab = 'all';
var currentModel = 'all';
var currentCollection = 'all';
var currentSort = 'filename';
var minConfidence = 0;
var currentLabelsFingerprint = null;  // set from ?labels_fingerprint= URL param
var currentPhotoIdFilter = null;  // set from ?photo_id= (deep link from Browse)
var availableModels = [];
var collectionPhotoIds = null;  // null = no filter
var thumbSize = 400;
var mode = 'review';
