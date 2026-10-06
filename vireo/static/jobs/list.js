// The job list pane: type filter, list items, and the running badge.
// Classic page script; load boot.js after all definitions.

function populateTypeFilter() {
  var sel = document.getElementById('filterType');
  var types = {};
  historyJobs.forEach(function(j) { types[j.type] = true; });
  activeJobs.forEach(function(j) { types[j.type] = true; });
  var current = sel.value;
  sel.innerHTML = '<option value="">All types</option>';
  Object.keys(types).sort().forEach(function(t) {
    sel.innerHTML += '<option value="' + esc(t) + '">' + esc(window.formatJobType(t)) + '</option>';
  });
  sel.value = current;
}
window.applyFilters = function() {
  updateList();
  if (currentView === 'history') renderHistoryOverview();
};

function updateList() {
  var container = document.getElementById('jobListContent');
  var typeFilter = document.getElementById('filterType').value;
  var statusFilter = document.getElementById('filterStatus').value;
  var html = '';

  // Active = running OR queued. Queued pipelines waiting behind a busy
  // slot are findable + cancellable here; without this, opening /jobs
  // after queuing from another tab would show the queued run nowhere.
  var running = activeJobs.filter(function(j) {
    return isLiveStatus(j.status);
  });
  html += '<div class="jobs-list-divider clickable' + (currentView === 'active' ? ' selected' : '') + '" data-view="active">Active' +
    (running.length > 0 ? ' (' + running.length + ')' : '') + '</div>';
  running.forEach(function(j) { html += renderListItem(j, 'active'); });

  var filtered = historyJobs;
  if (typeFilter) filtered = filtered.filter(function(j) { return j.type === typeFilter; });
  if (statusFilter) filtered = filtered.filter(function(j) { return j.status === statusFilter; });

  html += '<div class="jobs-list-divider clickable' + (currentView === 'history' ? ' selected' : '') + '" data-view="history">History</div>';
  if (filtered.length === 0 && running.length === 0) {
    html += '<div style="padding:24px;text-align:center;color:var(--text-ghost);font-size:13px;">No jobs yet</div>';
  }
  filtered.forEach(function(j) { html += renderListItem(j, 'history'); });
  container.innerHTML = html;
}

function renderListItem(j, source) {
  var selected = j.id === selectedJobId ? ' selected' : '';
  var dot = j.status || 'completed';
  var time = '';
  if (isLiveStatus(j.status) && j.started_at) {
    time = formatElapsed((Date.now() - new Date(j.started_at).getTime()) / 1000);
  } else if (j.started_at) {
    time = timeAgo(j.started_at);
  }
  var detail = '';
  var visibleProgress = activeProgress(j.progress);
  var collectionText = jobCollectionText(j);
  if (isLiveStatus(j.status) && j.progress && j.progress.phase) {
    detail = esc(j.progress.phase);
    if (visibleProgress && visibleProgress.phase) {
      detail += ' ' + visibleProgress.current.toLocaleString() + '/' + visibleProgress.total.toLocaleString();
    }
  } else if (j.summary) {
    detail = esc(j.summary);
  } else if (j.status === 'failed' && j.errors && j.errors.length > 0) {
    detail = '<span style="color:var(--danger);">' + esc(j.errors[0]).substring(0, 60) + '</span>';
  } else if (j.duration) {
    detail = formatElapsed(j.duration);
  }
  if (collectionText) {
    detail = detail ? detail + ' &middot; ' + esc(collectionText) : esc(collectionText);
  }
  var pct = 0;
  if (visibleProgress) {
    pct = progressPct(visibleProgress);
  }
  var html = '<div class="job-list-item' + selected + '" data-job-id="' + esc(j.id) + '" data-job-source="' + source + '">';
  html += '<div class="job-list-item-header">';
  html += '<span class="job-list-item-dot ' + dot + '"></span>';
  html += '<span class="job-list-item-type">' + esc(window.formatJobType(j.type)) + '</span>';
  html += '<span class="job-list-item-time">' + time + '</span>';
  html += '</div>';
  if (detail) html += '<div class="job-list-item-detail">' + detail + '</div>';
  if (isLiveStatus(j.status) && pct > 0) {
    html += '<div class="job-list-item-progress"><div class="job-list-item-progress-fill" style="width:' + pct + '%"></div></div>';
  }
  html += '</div>';
  return html;
}

function updateRunningBadge() {
  var running = activeJobs.filter(function(j) {
    return isLiveStatus(j.status);
  });
  var badge = document.getElementById('runningBadge');
  if (running.length > 0) { badge.textContent = running.length; badge.style.display = ''; }
  else { badge.style.display = 'none'; }
}

// Event delegation for job list clicks
function bindJobListClicks() {
  document.getElementById('jobListContent').addEventListener('click', function(e) {
    var viewItem = e.target.closest('[data-view]');
    if (viewItem) {
      currentView = viewItem.getAttribute('data-view');
      selectedJobId = null;
      selectedSource = null;
      if (sseSource) { sseSource.close(); sseSource = null; }
      renderMainPane();
      updateList();
      return;
    }
    var item = e.target.closest('[data-job-id]');
    if (item) selectJob(item.getAttribute('data-job-id'), item.getAttribute('data-job-source'));
  });
}
