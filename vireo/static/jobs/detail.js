// The detail pane: one job, step toggling, and the Active/History overviews.
// Classic page script; load boot.js after all definitions.

function renderDetail(job) {
  var pane = document.getElementById('jobDetailPane');
  pane.innerHTML = '<div style="padding:16px 24px 0;">' + renderJobCard(job) + '</div>';
  var activeStep = pane.querySelector('.tree-status-icon.running');
  if (activeStep) activeStep.closest('.tree-step').scrollIntoView({ block: 'nearest' });
}

function toggleStep(stepId) {
  if (collapsedSteps[stepId] === undefined) collapsedSteps[stepId] = true;
  else collapsedSteps[stepId] = !collapsedSteps[stepId];
  var job = activeJobs.find(function(j) { return j.id === selectedJobId; });
  if (job) renderDetail(job);
  else {
    job = historyJobs.find(function(j) { return j.id === selectedJobId; });
    if (job) renderHistoryDetail(job);
  }
}

function renderHistoryDetail(job) {
  var pane = document.getElementById('jobDetailPane');
  pane.innerHTML = '<div style="padding:16px 24px 0;">' + renderJobCard(job) + '</div>';
}

function renderMainPane() {
  if (currentView === 'active') {
    renderActiveOverview();
  } else if (currentView === 'history') {
    renderHistoryOverview();
  }
  // Individual job selection handled by existing selectJob flow
}

function renderActiveOverview() {
  var pane = document.getElementById('jobDetailPane');
  var running = activeJobs.filter(function(j) {
    return isLiveStatus(j.status);
  });

  if (running.length === 0) {
    pane.innerHTML = '<div class="jobs-overview-empty">No active jobs.<br><span style="font-size:12px;margin-top:8px;display:inline-block;">Import new photos from Import, process workspace photos from Process, or run individual jobs from Browse.</span></div>';
    return;
  }

  // A null workspace_id means the job is workspace-agnostic (the startup
  // working-copy / thumb_path backfills operate on photos, which are
  // global). Those belong with the current workspace's jobs — they are
  // running for this workspace as much as any other — not under "Other
  // Workspaces", and their badge has to say so rather than naming a
  // workspace they don't have.
  var thisWs = running.filter(function(j) {
    return j.workspace_id == null || j.workspace_id === activeWsId;
  });
  var otherWs = running.filter(function(j) {
    return j.workspace_id != null && j.workspace_id !== activeWsId;
  });

  var html = '<div style="padding:16px 24px 0;">';

  thisWs.forEach(function(j) {
    html += renderJobCard(j, j.workspace_id == null ? { workspaceName: 'All workspaces' } : {});
  });

  if (otherWs.length > 0) {
    html += '<div class="jobs-overview-divider">Other Workspaces</div>';
    otherWs.forEach(function(j) {
      var wsName = workspaceNames[j.workspace_id] || ('Workspace ' + j.workspace_id);
      html += renderJobCard(j, { workspaceName: wsName });
    });
  }

  html += '</div>';
  pane.innerHTML = html;
}

function renderHistoryOverview() {
  var pane = document.getElementById('jobDetailPane');
  var typeFilter = document.getElementById('filterType').value;
  var statusFilter = document.getElementById('filterStatus').value;

  var filtered = historyJobs;
  if (typeFilter) filtered = filtered.filter(function(j) { return j.type === typeFilter; });
  if (statusFilter) filtered = filtered.filter(function(j) { return j.status === statusFilter; });

  if (filtered.length === 0) {
    pane.innerHTML = '<div class="jobs-overview-empty">No job history' +
      (typeFilter || statusFilter ? ' matching filters' : '') + '.</div>';
    return;
  }

  // Date range note
  var oldest = filtered[filtered.length - 1];
  var oldestDate = new Date(oldest.started_at).toLocaleDateString();
  var html = '<div style="padding:16px 24px 0;">';
  html += '<div style="font-size:12px;color:var(--text-ghost);margin-bottom:12px;">Showing ' +
    filtered.length + ' jobs back to ' + oldestDate + '</div>';

  filtered.forEach(function(j) {
    html += renderJobCard(j);
  });

  html += '</div>';
  pane.innerHTML = html;
}
