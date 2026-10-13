// Fetching active jobs and history from the server.
// Classic page script; load boot.js after all definitions.

function refreshHistoryImportRecovery(job) {
  if (job.type !== 'import' || job.status !== 'failed' || !job.result ||
      !job.result.interrupted || !Array.isArray(job.result.photo_ids)) return;
  var cfg = jobConfig(job);
  var ids = importChainProcessJobIds(cfg, job.result, historyJobs, job.id);
  var missing = ids.some(function(id) {
    return !historyJobs.some(function(row) { return row.id === id; });
  });
  // No receipt can mean the checkpoint preceded the processing handoff.
  // The server also searches pipelines by chained_from in that crash window.
  if (!missing && ids.length) return;
  job.import_resume_pending = true;
  fetch('/api/import/' + encodeURIComponent(job.id) + '/recovery-options', {cache: 'no-store'})
    .then(function(response) {
      if (!response.ok) return null;
      return response.json();
    })
    .then(function(options) {
      // A subsequent history refresh or selection must not receive stale eligibility.
      if (!options || typeof options.resume_allowed !== 'boolean' ||
          historyJobs.indexOf(job) < 0) return;
      job.import_resume_allowed = options.resume_allowed;
      job.import_resume_pending = false;
      if (selectedSource === 'history' && selectedJobId === job.id) renderHistoryDetail(job);
      else if (currentView === 'history') renderHistoryOverview();
    })
    .catch(function() {});
}

function fetchJobs() {
  fetch('/api/jobs')
    .then(function(r) { return r.json(); })
    .then(function(data) {
      activeJobs = data.active || [];
      activeWsId = data.active_workspace_id || null;
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
            fetchHistory(true);
          }
        }
      } else if (currentView === 'active') {
        renderActiveOverview();
      }
    })
    .catch(function() {});
}

function fetchHistory(refreshMissingSelection) {
  fetch('/api/jobs/history?limit=100')
    .then(function(r) { return r.json(); })
    .then(function(data) {
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
      if (refreshMissingSelection && selectedSource === 'active' && selectedJobId) {
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
    })
    .catch(function() {});
}
