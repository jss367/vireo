// Fetching active jobs and history from the server.

import { state } from './state.js';
import { renderActiveOverview, renderDetail, renderHistoryDetail, renderHistoryOverview } from './detail.js';
import { jobConfig } from './format.js';
import { importChainProcessJobIds } from './import-retry.js';
import { populateTypeFilter, updateList, updateRunningBadge } from './list.js';
import { canRetryImportDirectToNas } from './shared.js';

export function renderJobsPollWarning() {
  var warning = /** @type {HTMLElement} */ (document.getElementById('jobsPollWarning'));
  if (!warning) return;
  var failed = [/** @type {const} */ ('active'), /** @type {const} */ ('history')].filter(function(name) { return state.jobsPollState[name].error; });
  var messages = failed.map(function(name) {
    var lane = state.jobsPollState[name];
    var label = name === 'active' ? 'Active jobs' : 'Job history';
    return label + (lane.loaded
      ? ' could not be refreshed. Showing the last successful list.'
      : ' could not be loaded.') + (lane.errorDetail ? ' ' + lane.errorDetail : '');
  });
  var message = messages.length ? messages.join(' ') + ' Retrying automatically.' : '';
  var status = /** @type {HTMLElement} */ (document.getElementById('jobsPollMessage'));
  // Successful polls in the other lane must not repeatedly announce this error.
  if (status.textContent !== message) status.textContent = message;
  warning.hidden = failed.length === 0;
  var retry = /** @type {HTMLButtonElement} */ (document.getElementById('retryJobsPolling'));
  retry.disabled = failed.length > 0 && failed.every(function(name) { return state.jobsPollState[name].pending; });
  retry.textContent = retry.disabled ? 'Retrying…' : 'Retry now';
}

export function retryJobsPolling() {
  if (state.jobsPollState.active.error && !state.jobsPollState.active.pending) fetchJobs();
  if (state.jobsPollState.history.error && !state.jobsPollState.history.pending) fetchHistory();
}

/** @param {unknown} value @returns {value is Record<string,unknown>} */ export function isJobsRecord(value) {
  return value !== null && typeof value === 'object' && !Array.isArray(value);
}

/** @param {unknown} rows @returns {asserts rows is JobsRow[]} */ export function validateJobRows(rows) {
  if (!Array.isArray(rows) || !rows.every(function(job) {
    return isJobsRecord(job) && typeof job.id === 'string' && job.id.length > 0 &&
      typeof job.type === 'string' && typeof job.status === 'string';
  })) throw new Error('Invalid jobs list');
}

/** @param {unknown} data @returns {asserts data is JobsSnapshot} */ export function validateActiveJobs(data) {
  if (!isJobsRecord(data)) throw new Error('Invalid active jobs response');
  validateJobRows(data.active);
  if (data.workspace_names !== undefined && (!isJobsRecord(data.workspace_names) ||
      !Object.values(data.workspace_names).every(function(name) { return typeof name === 'string'; }))) {
    throw new Error('Invalid workspace names');
  }
  if (data.keeping_awake !== undefined && typeof data.keeping_awake !== 'boolean') {
    throw new Error('Invalid keeping-awake status');
  }
  if (data.active_workspace_id != null && typeof data.active_workspace_id !== 'number') {
    throw new Error('Invalid active workspace');
  }
}

/** The server's own account of a failed jobs request ('' when it gave none).
 * @param {{json: () => Promise<unknown>}} response @returns {Promise<string>} */
export async function jobsServerError(response) {
  try {
    var body = await response.json();
    return isJobsRecord(body) && typeof body.error === 'string' ? body.error : '';
  } catch (_) {
    return '';
  }
}

/** @template T @param {'active'|'history'} name @param {string} url @param {(data:unknown)=>asserts data is T} validate @param {(data:T)=>void} apply @param {boolean=} background */ export async function fetchJobsSnapshot(name, url, validate, apply, background) {
  var lane = state.jobsPollState[name];
  if (background && lane.pending) return false;
  var generation = ++lane.generation;
  if (lane.controller) lane.controller.abort();
  var controller = new AbortController();
  lane.controller = controller;
  lane.pending = true;
  renderJobsPollWarning();
  // A hung connection must eventually release the lane for automatic/manual retry.
  var timeout = setTimeout(function() { controller.abort(); }, 15000);
  // E.g. "Vireo could not read the job history from its catalog (...)":
  // shown with the warning so a read failure is not mistaken for a network one.
  var errorDetail = '';
  try {
    var response = await fetch(url, {cache: 'no-store', signal: controller.signal});
    if (!response.ok) {
      errorDetail = await jobsServerError(response);
      throw new Error('Jobs request failed');
    }
    var data = await response.json();
    if (generation !== lane.generation) return false;
    if (controller.signal.aborted) throw new Error('Jobs request timed out');
    validate(data);
    lane.loaded = true;
    lane.error = false;
    lane.errorDetail = '';
    apply(data);
    return true;
  } catch (_) {
    if (generation === lane.generation) {
      lane.error = true;
      lane.errorDetail = errorDetail;
      // A failed first load is unknown, rather than an empty job list.
      if (!lane.loaded) {
        updateList();
        if (name === 'active' && state.currentView === 'active') renderActiveOverview();
        if (name === 'history' && state.currentView === 'history') renderHistoryOverview();
      }
    }
    return false;
  } finally {
    clearTimeout(timeout);
    if (generation === lane.generation) {
      lane.pending = false;
      lane.controller = null;
      renderJobsPollWarning();
    }
  }
}

/** @param {string} jobId */ export function fetchImportRecoveryOptions(jobId) {
  return fetch('/api/import/' + encodeURIComponent(jobId) + '/recovery-options', {cache: 'no-store'})
    .then(function(response) {
      return response.json().then(function(options) {
        if (!response.ok) {
          return Object.assign({}, options, {resume_allowed: false, direct_to_nas_allowed: false,
            error: options.error || 'Could not check import recovery options.', retryable: response.status >= 500});
        }
        if (!options || typeof options.resume_allowed !== 'boolean' ||
            typeof options.direct_to_nas_allowed !== 'boolean') throw new Error('Invalid import recovery options');
        return options;
      });
    })
    .catch(function() {
      return {resume_allowed: false, direct_to_nas_allowed: false,
        error: 'Could not check import recovery options.', retryable: true};
    });
}

/** @param {JobsRow} job */ export function refreshHistoryImportRecovery(job) {
  if (job.import_resume_pending || job.import_direct_pending) return;
  if (job.type !== 'import' || job.status !== 'failed' || !job.result) return;
  var interrupted = job.result.interrupted && Array.isArray(job.result.photo_ids);
  var directCandidate = canRetryImportDirectToNas(job, state.historyJobs);
  if (!interrupted && !directCandidate) return;
  var cfg = jobConfig(job);
  var ids = importChainProcessJobIds(cfg, job.result, state.historyJobs, job.id);
  var missing = ids.some(function(id) {
    return !state.historyJobs.some(function(row) { return row.id === id; });
  });
  // No receipt can mean the checkpoint preceded the processing handoff.
  // The server also searches pipelines by parent_job_id in that crash window.
  if (!directCandidate && !missing && ids.length) return;
  job.import_resume_pending = !!interrupted;
  job.import_direct_pending = directCandidate;
  /** @param {JobsRecoveryOptions} options */ function applyOptions(options) {
    // A subsequent history refresh or selection must not receive stale eligibility.
    if (state.historyJobs.indexOf(job) < 0) return;
    job.import_resume_allowed = options.resume_allowed;
    job.import_resume_pending = false;
    job.import_direct_pending = false;
    job.import_recovery_options = options;
    if (state.selectedSource === 'history' && state.selectedJobId === job.id) renderHistoryDetail(job);
    else if (state.currentView === 'history') renderHistoryOverview();
  }
  fetchImportRecoveryOptions(job.id).then(applyOptions);
}

/** @param {boolean=} background */ export function fetchJobs(background) {
  return fetchJobsSnapshot('active', '/api/jobs', validateActiveJobs, function(data) {
    state.activeJobs = data.active || [];
    state.activeWsId = data.active_workspace_id ?? null;
    state.workspaceNames = data.workspace_names || {};
    var awakeNote = /** @type {HTMLElement} */ (document.getElementById('keepingAwakeNote'));
    if (awakeNote) {
      awakeNote.style.display = data.keeping_awake ? '' : 'none';
    }
    // Drop optimistic-cancel state for jobs that have left the active
    // list (server has finalized them; their next render comes from
    // historyJobs which never shows a cancel button).
    /** @type {Record<string,boolean>} */ var stillActive = {};
    state.activeJobs.forEach(function(j) { stillActive[j.id] = true; });
    Object.keys(state.cancellingJobIds).forEach(function(id) {
      if (!stillActive[id]) delete state.cancellingJobIds[id];
    });
    updateList();
    updateRunningBadge();
    if (state.selectedSource === 'active' && state.selectedJobId) {
      var selected = state.activeJobs.find(function(j) { return j.id === state.selectedJobId; });
      if (selected) {
        renderDetail(selected);
      } else {
        var historical = state.historyJobs.find(function(j) { return j.id === state.selectedJobId; });
        if (historical) {
          state.selectedSource = 'history';
          renderHistoryDetail(historical);
        } else {
          fetchHistory(true, true);
        }
      }
    } else if (state.currentView === 'active') {
      renderActiveOverview();
    }
  }, background);
}

/** @param {boolean=} refreshMissingSelection @param {boolean=} background */ export function fetchHistory(refreshMissingSelection, background) {
  if (refreshMissingSelection) {
    // The first handoff needs a snapshot requested after the job disappeared.
    // Later active polls must not keep aborting that history request.
    if (state.historyRefreshSelectionId !== state.selectedJobId) background = false;
    state.historyRefreshSelectionId = state.selectedJobId;
  }
  return fetchJobsSnapshot('history', '/api/jobs/history?limit=100', validateJobRows, function(data) {
    state.historyJobs = data || [];
    state.historyJobs.forEach(function(j) {
      if (typeof j.tree === 'string') {
        try { j.tree = JSON.parse(j.tree); } catch(e) { j.tree = []; }
      }
      if (typeof j.result === 'string') {
        try { j.result = JSON.parse(j.result); } catch(e) { j.result = null; }
      }
    });
    updateList();
    populateTypeFilter();
    state.historyJobs.forEach(refreshHistoryImportRecovery);
    var reconcileId = state.historyRefreshSelectionId;
    state.historyRefreshSelectionId = null;
    if (reconcileId && state.selectedSource === 'active' && state.selectedJobId === reconcileId &&
        !state.activeJobs.some(function(job) { return job.id === reconcileId; })) {
      var historical = state.historyJobs.find(function(j) { return j.id === state.selectedJobId; });
      if (historical) {
        state.selectedSource = 'history';
        renderHistoryDetail(historical);
      } else {
        state.currentView = 'active';
        state.selectedJobId = null;
        state.selectedSource = null;
        renderActiveOverview();
        updateList();
      }
      return;
    }
    if (state.currentView === 'history') renderHistoryOverview();
  }, background);
}
