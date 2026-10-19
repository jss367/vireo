// ID Conflicts page state shared by its classic scripts: the comparison
// page on screen, paging, the models shown, filters, sort and excludes, the
// selection, the request sequences, and the saved conflict threshold.
// Classic page script; loads first among the ID Conflicts scripts. It reads
// the saved sort, page size and threshold while it loads.

// The comparison itself is derived on the server (see vireo/id_conflicts.py):
// working out what a row means costs a pass over every prediction in the
// collection and the taxonomy behind it, and a catalog-sized collection
// produces tens of thousands of rows. This page asks for one page of rows
// plus the counts, and renders what it is told.
/** @type {CompareResponse | null} */
var compareData = null;
/** @type {ComparePhoto[]} */
var compareRows = [];
/** @type {string | null} */
var compareToken = null;
var comparePage = 1;
var comparePerPage = 60;
var compareTotal = 0;
/** @type {string[]} */
var compareModels = [];
/** @type {CompareChip[]} */
var compareFilterDefs = [];
/** @type {CompareChip[]} */
var compareSortDefs = [];
/** @type {CompareChip[]} */
var compareExcludeDefs = [];
var activeFilter = 'needs_review';
var activeSort = 'review_priority';
/** @type {Record<string, boolean>} */
var excludeState = {};
/** @type {Set<number>} */
var selectedRows = new Set();
// Bumped by full collection loads only. Scoped decision refreshes read it to
// tell whether the comparison they were queued against is still on screen.
var loadingSeq = 0;
// Bumped by every row request, so a slower response can never paint over a
// newer one — a filter click and a queued decision refresh race constantly.
var requestSeq = 0;
/** @type {Promise<unknown>} */
var decisionRefreshQueue = Promise.resolve();
/** @type {ReturnType<typeof setTimeout> | null} */
var searchTimer = null;
var CONFLICT_THRESHOLD_KEY = 'vireo.compare.conflictThreshold';
var PER_PAGE_KEY = 'vireo.compare.perPage';
var conflictMinConfidence = loadConflictThreshold();

var savedSort = VireoViewPreferences.read('vireo.compare.sort');
if (savedSort) activeSort = savedSort;
var savedPerPage = parseInt(VireoViewPreferences.read(PER_PAGE_KEY) || '', 10);
if (savedPerPage > 0) comparePerPage = savedPerPage;

/** @param {string | number} value */
function clampConfidence(value) {
  var parsed = parseFloat(String(value));
  if (!Number.isFinite(parsed)) return 0.4;
  return Math.min(1, Math.max(0, parsed));
}

function loadConflictThreshold() {
  try {
    var saved = window.localStorage && window.localStorage.getItem(CONFLICT_THRESHOLD_KEY);
    if (saved !== null) return clampConfidence(saved);
  } catch (_e) {}
  return 0.4;
}

function updateConflictThresholdControl() {
  var input = /** @type {HTMLInputElement} */ (document.getElementById('conflictThreshold'));
  var value = /** @type {HTMLElement} */ (document.getElementById('conflictThresholdValue'));
  if (input) input.value = String(conflictMinConfidence);
  if (value) value.textContent = Math.round(conflictMinConfidence * 100) + '%';
}

// The slider's label follows the drag, but the comparison is only re-derived
// when the drag ends: the threshold changes what every row *means*, so it
// rebuilds the whole assessment rather than re-filtering a list.
/** @param {string} value */
function previewConflictThreshold(value) {
  var label = /** @type {HTMLElement} */ (document.getElementById('conflictThresholdValue'));
  if (label) label.textContent = Math.round(clampConfidence(value) * 100) + '%';
}

/** @param {string} value */
function setConflictThreshold(value) {
  var next = clampConfidence(value);
  if (next === conflictMinConfidence) return;
  conflictMinConfidence = next;
  try {
    if (window.localStorage) {
      window.localStorage.setItem(CONFLICT_THRESHOLD_KEY, String(conflictMinConfidence));
    }
  } catch (_e) {}
  updateConflictThresholdControl();
  reloadRows();
}
