// ID Conflicts page state: the comparison page on screen, paging, the models
// shown, filters, sort and excludes, the selection, the request sequences,
// and the saved conflict threshold. ES module: the page's other modules
// import `state` and read and write its fields; nothing here is a global. It
// reads the saved sort, page size and threshold while it loads.
//
// The comparison itself is derived on the server (see vireo/id_conflicts.py):
// working out what a row means costs a pass over every prediction in the
// collection and the taxonomy behind it, and a catalog-sized collection
// produces tens of thousands of rows. This page asks for one page of rows
// plus the counts, and renders what it is told.

import { VireoViewPreferences } from './shared.js';

export const CONFLICT_THRESHOLD_KEY = 'vireo.compare.conflictThreshold';
export const PER_PAGE_KEY = 'vireo.compare.perPage';

/** @param {string | number} value */
export function clampConfidence(value) {
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

/**
 * @typedef {object} CompareState
 * @property {CompareResponse | null} compareData
 * @property {ComparePhoto[]} compareRows
 * @property {string | null} compareToken
 * @property {number} comparePage
 * @property {number} comparePerPage
 * @property {number} compareTotal
 * @property {string[]} compareModels
 * @property {CompareChip[]} compareFilterDefs
 * @property {CompareChip[]} compareSortDefs
 * @property {CompareChip[]} compareExcludeDefs
 * @property {string} activeFilter
 * @property {string} activeSort
 * @property {Record<string, boolean>} excludeState
 * @property {Set<number>} selectedRows
 * @property {number} loadingSeq
 *   Bumped by full collection loads only. Scoped decision refreshes read it
 *   to tell whether the comparison they were queued against is still on
 *   screen.
 * @property {number} requestSeq
 *   Bumped by every row request, so a slower response can never paint over
 *   a newer one — a filter click and a queued decision refresh race
 *   constantly.
 * @property {Promise<unknown>} decisionRefreshQueue
 * @property {ReturnType<typeof setTimeout> | null} searchTimer
 * @property {number} conflictMinConfidence
 */

/** @type {CompareState} */
export const state = {
  compareData: null,
  compareRows: [],
  compareToken: null,
  comparePage: 1,
  comparePerPage: 60,
  compareTotal: 0,
  compareModels: [],
  compareFilterDefs: [],
  compareSortDefs: [],
  compareExcludeDefs: [],
  activeFilter: 'needs_review',
  activeSort: 'review_priority',
  excludeState: {},
  selectedRows: new Set(),
  loadingSeq: 0,
  requestSeq: 0,
  decisionRefreshQueue: Promise.resolve(),
  searchTimer: null,
  conflictMinConfidence: loadConflictThreshold(),
};

var savedSort = VireoViewPreferences.read('vireo.compare.sort');
if (savedSort) state.activeSort = savedSort;
var savedPerPage = parseInt(VireoViewPreferences.read(PER_PAGE_KEY) || '', 10);
if (savedPerPage > 0) state.comparePerPage = savedPerPage;
