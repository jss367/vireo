// Fetching active jobs and history from the server.
// Classic page script; load boot.js after all definitions.

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
