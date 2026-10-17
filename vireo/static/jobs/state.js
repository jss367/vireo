// Shared Jobs page state: job lists, the selection, and per-step UI state.
// Classic page script; load boot.js after all definitions.

/** @type {JobsRow[]} */ var activeJobs = [];
/** @type {JobsRow[]} */ var historyJobs = [];
/** @type {string | null} */ var selectedJobId = null;
/** @type {string | null} */ var selectedSource = null;
/** @type {ReturnType<typeof setInterval> | null} */ var pollTimer = null;
// Active jobs and history refresh independently. A new explicit refresh retires
// its lane's older request; scheduled polls wait for that lane to settle.
/** @type {Record<'active'|'history',JobsPollLane>} */ var jobsPollState = {
  active: {generation: 0, pending: false, loaded: false, error: false, controller: null},
  history: {generation: 0, pending: false, loaded: false, error: false, controller: null},
};
/** @type {string | null} */ var historyRefreshSelectionId = null;
/** @type {EventSource | null} */ var sseSource = null;
/** @type {Record<string,{name:string;status:string}[]>} */ var leafBuffers = {};
/** @type {Record<string,number>} */ var leafBufferSources = {};
var LEAF_MAX = 20;
// Job ids the user has clicked Cancel on but the server hasn't yet
// moved out of the active list. Tracked here so subsequent renders
// (every 2s poll) keep showing the optimistic "Cancelling…" state
// instead of reverting to a fresh "Cancel" button.
/** @type {Record<string,boolean>} */ var cancellingJobIds = {};
// Preserve pending restart controls across polling redraws.
/** @type {Record<string,{pending?:boolean;jobId?:string}>} */ var inPlaceRestarts = {};
/** @type {Record<string,boolean>} */ var pendingImportRetries = {};
/** @type {Record<string,boolean>} */ var collapsedSteps = {};
/** @type {number | null} */ var activeWsId = null;
/** @type {Record<number,string>} */ var workspaceNames = {};
var currentView = 'active'; // 'active', 'history', or a job id

/** Require a template-owned element and verify its DOM interface.
 * @template {Element} T
 * @param {string} id
 * @param {{new (...args: never[]): T}} constructor
 * @returns {T}
 */
function jobsElement(id, constructor) {
  var element = document.getElementById(id);
  if (!(element instanceof constructor)) throw new Error('Missing Jobs element: ' + id);
  return element;
}
