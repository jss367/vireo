// Selecting a job and following it over its progress stream.

import { state } from './state.js';
import { renderDetail, renderHistoryDetail } from './detail.js';
import { isLiveStatus } from './format.js';
import { updateList, updateRunningBadge } from './list.js';
import { fetchHistory, fetchJobs } from './polling.js';

/** @param {string} jobId @param {string} source */ export function selectJob(jobId, source) {
  state.currentView = jobId;
  state.selectedJobId = jobId;
  state.selectedSource = source;
  state.leafBuffers = {};
  state.leafBufferSources = {};
  state.collapsedSteps = {};
  if (state.sseSource) { state.sseSource.close(); state.sseSource = null; }
  if (source === 'active') {
    var job = state.activeJobs.find(function(j) { return j.id === jobId; });
    if (job) { renderDetail(job); if (isLiveStatus(job.status)) connectSSE(jobId); }
  } else {
    job = state.historyJobs.find(function(j) { return j.id === jobId; });
    if (job) renderHistoryDetail(job);
  }
  updateList();
}

/** @param {string} jobId */ export function connectSSE(jobId) {
  if (state.sseSource) state.sseSource.close();
  state.sseSource = new EventSource('/api/jobs/' + jobId + '/stream');
  state.sseSource.addEventListener('progress', function(e) {
    var data = JSON.parse(e.data);
    var job = state.activeJobs.find(function(j) { return j.id === jobId; });
    if (job) {
      job.progress = data;
      if (data.steps) job.steps = data.steps;
      if (state.selectedJobId === jobId) renderDetail(job);
      updateList();
    }
  });
  state.sseSource.addEventListener('status', function(e) {
    var data = JSON.parse(e.data);
    var job = state.activeJobs.find(function(j) { return j.id === jobId; });
    if (job && data.status) {
      job.status = data.status;
      if (typeof data.pausable === 'boolean') job.pausable = data.pausable;
      if (typeof data.cancellable === 'boolean') job.cancellable = data.cancellable;
      if ('control_unavailable_reason' in data) job.control_unavailable_reason = data.control_unavailable_reason;
      if (state.selectedJobId === jobId) renderDetail(job);
      updateList();
      updateRunningBadge();
    }
  });
  state.sseSource.addEventListener('complete', function(e) {
    /** @type {Partial<JobsRow>} */ var data = {};
    try { data = JSON.parse(e.data); } catch(ex) {}
    var job = state.activeJobs.find(function(j) { return j.id === jobId; });
    if (job) {
      job.status = data.status || job.status;
      if (data.result !== undefined) job.result = data.result;
      if (data.duration !== undefined) job.duration = data.duration;
      if (data.errors !== undefined) job.errors = data.errors;
      if (state.selectedJobId === jobId) renderDetail(job);
    }
    if (state.sseSource) { state.sseSource.close(); state.sseSource = null; }
    fetchJobs();
    fetchHistory();
  });
  state.sseSource.addEventListener('error', function() {
    if (state.sseSource) { state.sseSource.close(); state.sseSource = null; }
  });
}
