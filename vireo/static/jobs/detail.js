// The detail pane: one job, step toggling, and the Active/History overviews.

import { jobsElement, state } from './state.js';
import { isLiveStatus } from './format.js';
import { renderJobCard } from './job-card.js';
import { escapeHtml } from './shared.js';

/** @param {JobsRow} job */ export function renderDetail(job) {
  var pane = jobsElement('jobDetailPane', HTMLElement);
  pane.innerHTML = '<div style="padding:16px 24px 0;">' + renderJobCard(job) + '</div>';
  var activeStep = pane.querySelector('.tree-status-icon.running');
  if (activeStep) activeStep.closest('.tree-step')?.scrollIntoView({ block: 'nearest' });
}

/** @param {string} stepId */ export function toggleStep(stepId) {
  if (state.collapsedSteps[stepId] === undefined) state.collapsedSteps[stepId] = true;
  else state.collapsedSteps[stepId] = !state.collapsedSteps[stepId];
  var job = state.activeJobs.find(function(j) { return j.id === state.selectedJobId; });
  if (job) renderDetail(job);
  else {
    job = state.historyJobs.find(function(j) { return j.id === state.selectedJobId; });
    if (job) renderHistoryDetail(job);
  }
}

/** @param {JobsRow} job */ export function renderHistoryDetail(job) {
  var pane = jobsElement('jobDetailPane', HTMLElement);
  pane.innerHTML = '<div style="padding:16px 24px 0;">' + renderJobCard(job) + '</div>';
}

export function renderMainPane() {
  if (state.currentView === 'active') {
    renderActiveOverview();
  } else if (state.currentView === 'history') {
    renderHistoryOverview();
  }
  // Individual job selection handled by existing selectJob flow
}

export function renderActiveOverview() {
  var pane = jobsElement('jobDetailPane', HTMLElement);
  if (!state.jobsPollState.active.loaded) {
    pane.innerHTML = '<div class="jobs-overview-empty">' +
      (state.jobsPollState.active.error ? 'Active jobs could not be loaded. Retrying automatically.' : 'Loading active jobs…') + '</div>';
    return;
  }
  var running = state.activeJobs.filter(function(j) {
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
    return j.workspace_id == null || j.workspace_id === state.activeWsId;
  });
  var otherWs = running.filter(function(j) {
    return j.workspace_id != null && j.workspace_id !== state.activeWsId;
  });

  var html = '<div style="padding:16px 24px 0;">';

  thisWs.forEach(function(j) {
    html += renderJobCard(j, j.workspace_id == null ? { workspaceName: 'All workspaces' } : {});
  });

  if (otherWs.length > 0) {
    html += '<div class="jobs-overview-divider">Other Workspaces</div>';
    otherWs.forEach(function(j) {
      var wsName = state.workspaceNames[j.workspace_id ?? -1] || ('Workspace ' + j.workspace_id);
      html += renderJobCard(j, { workspaceName: wsName });
    });
  }

  html += '</div>';
  pane.innerHTML = html;
}

export function renderHistoryOverview() {
  var pane = jobsElement('jobDetailPane', HTMLElement);
  if (!state.jobsPollState.history.loaded) {
    pane.innerHTML = '<div class="jobs-overview-empty">' +
      (state.jobsPollState.history.error
        ? escapeHtml('Job history could not be loaded.' +
          (state.jobsPollState.history.errorDetail ? ' ' + state.jobsPollState.history.errorDetail : '') +
          ' Retrying automatically.')
        : 'Loading job history…') + '</div>';
    return;
  }
  var typeFilter = jobsElement('filterType', HTMLSelectElement).value;
  var statusFilter = jobsElement('filterStatus', HTMLSelectElement).value;

  var filtered = state.historyJobs;
  if (typeFilter) filtered = filtered.filter(function(j) { return j.type === typeFilter; });
  if (statusFilter) filtered = filtered.filter(function(j) { return j.status === statusFilter; });

  if (filtered.length === 0) {
    pane.innerHTML = '<div class="jobs-overview-empty">No job history' +
      (typeFilter || statusFilter ? ' matching filters' : '') + '.</div>';
    return;
  }

  // Date range note
  var oldest = filtered[filtered.length - 1];
  var oldestDate = new Date(oldest?.started_at || '').toLocaleDateString();
  var html = '<div style="padding:16px 24px 0;">';
  html += '<div style="font-size:12px;color:var(--text-ghost);margin-bottom:12px;">Showing ' +
    filtered.length + ' jobs back to ' + oldestDate + '</div>';

  filtered.forEach(function(j) {
    html += renderJobCard(j);
  });

  html += '</div>';
  pane.innerHTML = html;
}
