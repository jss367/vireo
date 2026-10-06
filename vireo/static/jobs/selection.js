// Selecting a job and following it over its progress stream.
// Classic page script; load boot.js after all definitions.

function selectJob(jobId, source) {
  currentView = jobId;
  selectedJobId = jobId;
  selectedSource = source;
  leafBuffers = {};
  leafBufferSources = {};
  collapsedSteps = {};
  if (sseSource) { sseSource.close(); sseSource = null; }
  if (source === 'active') {
    var job = activeJobs.find(function(j) { return j.id === jobId; });
    if (job) { renderDetail(job); if (isLiveStatus(job.status)) connectSSE(jobId); }
  } else {
    var job = historyJobs.find(function(j) { return j.id === jobId; });
    if (job) renderHistoryDetail(job);
  }
  updateList();
}

function connectSSE(jobId) {
  if (sseSource) sseSource.close();
  sseSource = new EventSource('/api/jobs/' + jobId + '/stream');
  sseSource.addEventListener('progress', function(e) {
    var data = JSON.parse(e.data);
    var job = activeJobs.find(function(j) { return j.id === jobId; });
    if (job) {
      job.progress = data;
      if (data.steps) job.steps = data.steps;
      if (selectedJobId === jobId) renderDetail(job);
      updateList();
    }
  });
  sseSource.addEventListener('status', function(e) {
    var data = JSON.parse(e.data);
    var job = activeJobs.find(function(j) { return j.id === jobId; });
    if (job && data.status) {
      job.status = data.status;
      if (selectedJobId === jobId) renderDetail(job);
      updateList();
      updateRunningBadge();
    }
  });
  sseSource.addEventListener('complete', function(e) {
    var data = {};
    try { data = JSON.parse(e.data); } catch(ex) {}
    var job = activeJobs.find(function(j) { return j.id === jobId; });
    if (job) {
      job.status = data.status || job.status;
      if (data.result !== undefined) job.result = data.result;
      if (data.duration !== undefined) job.duration = data.duration;
      if (data.errors !== undefined) job.errors = data.errors;
      if (selectedJobId === jobId) renderDetail(job);
    }
    if (sseSource) { sseSource.close(); sseSource = null; }
    fetchJobs();
    fetchHistory();
  });
  sseSource.onerror = function() {
    if (sseSource) { sseSource.close(); sseSource = null; }
  };
}
