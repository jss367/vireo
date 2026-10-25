// Jobs page state: job lists, the selection, polling, and per-step UI state.
// ES module: the page's other modules import `state` and read and write its
// fields; nothing here is a global.

/**
 * @typedef {object} JobsState
 * @property {JobsRow[]} activeJobs
 * @property {JobsRow[]} historyJobs
 * @property {string | null} selectedJobId
 * @property {string | null} selectedSource
 * @property {ReturnType<typeof setInterval> | null} pollTimer
 * @property {Record<'active'|'history', JobsPollLane>} jobsPollState
 *   Active jobs and history refresh independently. A new explicit refresh
 *   retires its lane's older request; scheduled polls wait for that lane to
 *   settle.
 * @property {string | null} historyRefreshSelectionId
 * @property {EventSource | null} sseSource
 * @property {Record<string, {name: string; status: string}[]>} leafBuffers
 * @property {Record<string, number>} leafBufferSources
 * @property {Record<string, boolean>} cancellingJobIds
 *   Job ids the user has clicked Cancel on but the server hasn't yet moved
 *   out of the active list. Tracked here so subsequent renders (every 2s
 *   poll) keep showing the optimistic "Cancelling…" state instead of
 *   reverting to a fresh "Cancel" button.
 * @property {Record<string, {pending?: boolean; jobId?: string}>} inPlaceRestarts
 *   Pending restart controls, preserved across polling redraws.
 * @property {Record<string, boolean>} pendingImportRetries
 * @property {Record<string, JobsSourceCleanup>} sourceCleanupReviews
 *   Original-folder cleanup after a date-organized move, per job.
 * @property {Record<string, boolean>} collapsedSteps
 * @property {number | null} activeWsId
 * @property {Record<number, string>} workspaceNames
 * @property {string} currentView
 *   'active', 'history', or a job id.
 */

/** @returns {JobsPollLane} */
function pollLane() {
  return {generation: 0, pending: false, loaded: false, error: false, errorDetail: '', controller: null};
}

/** @type {JobsState} */
export const state = {
  activeJobs: [],
  historyJobs: [],
  selectedJobId: null,
  selectedSource: null,
  pollTimer: null,
  jobsPollState: {active: pollLane(), history: pollLane()},
  historyRefreshSelectionId: null,
  sseSource: null,
  leafBuffers: {},
  leafBufferSources: {},
  cancellingJobIds: {},
  inPlaceRestarts: {},
  pendingImportRetries: {},
  sourceCleanupReviews: {},
  collapsedSteps: {},
  activeWsId: null,
  workspaceNames: {},
  currentView: 'active',
};

export const LEAF_MAX = 20;

/** Require a template-owned element and verify its DOM interface.
 * @template {Element} T
 * @param {string} id
 * @param {{new (...args: never[]): T}} constructor
 * @returns {T}
 */
export function jobsElement(id, constructor) {
  var element = document.getElementById(id);
  if (!(element instanceof constructor)) throw new Error('Missing Jobs element: ' + id);
  return element;
}
