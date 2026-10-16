// Fetching active jobs and history from the server.
// Classic page script; load boot.js after all definitions.

function renderJobsPollWarning() {
  var warning = document.getElementById('jobsPollWarning');
  if (!warning) return;
  var failed = Object.keys(jobsPollState).filter(function(name) { return jobsPollState[name].error; });
  var messages = failed.map(function(name) {
    var lane = jobsPollState[name];
    var label = name === 'active' ? 'Active jobs' : 'Job history';
    return label + (lane.loaded
      ? ' could not be refreshed. Showing the last successful list.'
      : ' could not be loaded.');
  });
  var message = messages.length ? messages.join(' ') + ' Retrying automatically.' : '';
  var status = document.getElementById('jobsPollMessage');
  // Successful polls in the other lane must not repeatedly announce this error.
  if (status.textContent !== message) status.textContent = message;
  warning.hidden = failed.length === 0;
  var retry = document.getElementById('retryJobsPolling');
  retry.disabled = failed.length > 0 && failed.every(function(name) { return jobsPollState[name].pending; });
  retry.textContent = retry.disabled ? 'Retrying…' : 'Retry now';
}

function retryJobsPolling() {
  if (jobsPollState.active.error && !jobsPollState.active.pending) fetchJobs();
  if (jobsPollState.history.error && !jobsPollState.history.pending) fetchHistory();
}

function isJobsRecord(value) {
  return value !== null && typeof value === 'object' && !Array.isArray(value);
}

function validateJobRows(rows) {
  if (!Array.isArray(rows) || !rows.every(function(job) {
    return isJobsRecord(job) && typeof job.id === 'string' && job.id.length > 0 &&
      typeof job.type === 'string' && typeof job.status === 'string';
  })) throw new Error('Invalid jobs list');
}

function validateActiveJobs(data) {
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

async function fetchJobsSnapshot(name, url, validate, apply, background) {
  var lane = jobsPollState[name];
  if (background && lane.pending) return false;
  var generation = ++lane.generation;
  if (lane.controller) lane.controller.abort();
  var controller = new AbortController();
  lane.controller = controller;
  lane.pending = true;
  renderJobsPollWarning();
  // A hung connection must eventually release the lane for automatic/manual retry.
  var timeout = setTimeout(function() { controller.abort(); }, 15000);
  try {
    var response = await fetch(url, {cache: 'no-store', signal: controller.signal});
    if (!response.ok) throw new Error('Jobs request failed');
    var data = await response.json();
    if (generation !== lane.generation) return false;
    if (controller.signal.aborted) throw new Error('Jobs request timed out');
    validate(data);
    lane.loaded = true;
    lane.error = false;
    apply(data);
    return true;
  } catch (_) {
    if (generation === lane.generation) {
      lane.error = true;
      // A failed first load is unknown, rather than an empty job list.
      if (!lane.loaded) {
        updateList();
        if (name === 'active' && currentView === 'active') renderActiveOverview();
        if (name === 'history' && currentView === 'history') renderHistoryOverview();
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

function fetchImportRecoveryOptions(jobId) {
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

function refreshHistoryImportRecovery(job) {
  if (job.import_resume_pending || job.import_direct_pending) return;
  if (job.type !== 'import' || job.status !== 'failed' || !job.result) return;
  var interrupted = job.result.interrupted && Array.isArray(job.result.photo_ids);
  var directCandidate = canRetryImportDirectToNas(job, historyJobs);
  if (!interrupted && !directCandidate) return;
  var cfg = jobConfig(job);
  var ids = importChainProcessJobIds(cfg, job.result, historyJobs, job.id);
  var missing = ids.some(function(id) {
    return !historyJobs.some(function(row) { return row.id === id; });
  });
  // No receipt can mean the checkpoint preceded the processing handoff.
  // The server also searches pipelines by chained_from in that crash window.
  if (!directCandidate && !missing && ids.length) return;
  job.import_resume_pending = !!interrupted;
  job.import_direct_pending = directCandidate;
  function applyOptions(options) {
    // A subsequent history refresh or selection must not receive stale eligibility.
    if (historyJobs.indexOf(job) < 0) return;
    job.import_resume_allowed = options.resume_allowed;
    job.import_resume_pending = false;
    job.import_direct_pending = false;
    job.import_recovery_options = options;
    if (selectedSource === 'history' && selectedJobId === job.id) renderHistoryDetail(job);
    else if (currentView === 'history') renderHistoryOverview();
  }
  fetchImportRecoveryOptions(job.id).then(applyOptions);
}

function fetchJobs(background) {
  return fetchJobsSnapshot('active', '/api/jobs', validateActiveJobs, function(data) {
    activeJobs = data.active || [];
    activeWsId = data.active_workspace_id ?? null;
    workspaceNames = data.workspace_names || {};
    var awakeNote = document.getElementById('keepingAwakeNote');
    if (awakeNote) {
      awakeNote.style.display = data.keeping_awake ? '' : 'none';
    }
    // Drop optimistic-cancel state for jobs that have left the active
    // list (server has finalized them; their next render comes from
    // historyJobs which never shows a cancel button).
    var stillActive = {};
    activeJobs.forEach(function(j) { stillActive[j.id] = true; });
    Object.keys(cancellingJobIds).forEach(function(id) {
      if (!stillActive[id]) delete cancellingJobIds[id];
    });
    updateList();
    updateRunningBadge();
    if (selectedSource === 'active' && selectedJobId) {
      var selected = activeJobs.find(function(j) { return j.id === selectedJobId; });
      if (selected) {
        renderDetail(selected);
      } else {
        var historical = historyJobs.find(function(j) { return j.id === selectedJobId; });
        if (historical) {
          selectedSource = 'history';
          renderHistoryDetail(historical);
        } else {
          fetchHistory(true, true);
        }
      }
    } else if (currentView === 'active') {
      renderActiveOverview();
    }
  }, background);
}

function fetchHistory(refreshMissingSelection, background) {
  if (refreshMissingSelection) {
    // The first handoff needs a snapshot requested after the job disappeared.
    // Later active polls must not keep aborting that history request.
    if (historyRefreshSelectionId !== selectedJobId) background = false;
    historyRefreshSelectionId = selectedJobId;
  }
  return fetchJobsSnapshot('history', '/api/jobs/history?limit=100', validateJobRows, function(data) {
    historyJobs = data || [];
    historyJobs.forEach(function(j) {
      if (typeof j.tree === 'string') {
        try { j.tree = JSON.parse(j.tree); } catch(e) { j.tree = []; }
      }
      if (typeof j.result === 'string') {
        try { j.result = JSON.parse(j.result); } catch(e) { j.result = null; }
      }
    });
    updateList();
    populateTypeFilter();
    historyJobs.forEach(refreshHistoryImportRecovery);
    var reconcileId = historyRefreshSelectionId;
    historyRefreshSelectionId = null;
    if (reconcileId && selectedSource === 'active' && selectedJobId === reconcileId &&
        !activeJobs.some(function(job) { return job.id === reconcileId; })) {
      var historical = historyJobs.find(function(j) { return j.id === selectedJobId; });
      if (historical) {
        selectedSource = 'history';
        renderHistoryDetail(historical);
      } else {
        currentView = 'active';
        selectedJobId = null;
        selectedSource = null;
        renderActiveOverview();
        updateList();
      }
      return;
    }
    if (currentView === 'history') renderHistoryOverview();
  }, background);
}
