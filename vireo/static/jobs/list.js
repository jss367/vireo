// The job list pane: type filter, list items, and the running badge.
// Classic page script; load boot.js after all definitions.

function populateTypeFilter() {
  var sel = jobsElement('filterType', HTMLSelectElement);
  /** @type {Record<string,boolean>} */ var types = {};
  historyJobs.forEach(function(j) { types[j.type] = true; });
  activeJobs.forEach(function(j) { types[j.type] = true; });
  var current = sel.value;
  sel.innerHTML = '<option value="">All types</option>';
  Object.keys(types).sort().forEach(function(t) {
    sel.innerHTML += '<option value="' + escapeHtml(t) + '">' + escapeHtml(window.formatJobType(t)) + '</option>';
  });
  sel.value = current;
}
window.applyFilters = function() {
  updateList();
  if (currentView === 'history') renderHistoryOverview();
};

function updateList() {
  var container = jobsElement('jobListContent', HTMLElement);
  var typeFilter = jobsElement('filterType', HTMLSelectElement).value;
  var statusFilter = jobsElement('filterStatus', HTMLSelectElement).value;
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
  if (jobsPollState.history.error && !jobsPollState.history.loaded) {
    // Never an empty History section that reads as "no jobs have run".
    html += '<div style="padding:12px 16px;color:var(--text-ghost);font-size:13px;">Job history could not be loaded.</div>';
  } else if (filtered.length === 0 && running.length === 0) {
    var emptyMessage = 'No jobs yet';
    if (!jobsPollState.active.loaded || !jobsPollState.history.loaded) {
      emptyMessage = jobsPollState.active.error || jobsPollState.history.error
        ? 'Some job lists could not be loaded.' : 'Loading jobs…';
    }
    html += '<div style="padding:24px;text-align:center;color:var(--text-ghost);font-size:13px;">' + emptyMessage + '</div>';
  }
  filtered.forEach(function(j) { html += renderListItem(j, 'history'); });
  container.innerHTML = html;
}

/** @param {JobsRow} j @param {string} source */ function renderListItem(j, source) {
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
    detail = escapeHtml(j.progress.phase);
    if (visibleProgress && visibleProgress.phase) {
      detail += ' ' + visibleProgress.current.toLocaleString() + '/' + visibleProgress.total.toLocaleString();
    }
  } else if (j.summary) {
    detail = escapeHtml(j.summary);
  } else if (j.status === 'failed' && j.errors && j.errors.length > 0) {
    detail = '<span style="color:var(--danger);">' + escapeHtml(j.errors[0]).substring(0, 60) + '</span>';
  } else if (j.duration) {
    detail = formatElapsed(j.duration);
  }
  if (collectionText) {
    detail = detail ? detail + ' &middot; ' + escapeHtml(collectionText) : escapeHtml(collectionText);
  }
  var pct = 0;
  if (visibleProgress) {
    pct = progressPct(visibleProgress);
  }
  var html = '<div class="job-list-item' + selected + '" data-job-id="' + escapeHtml(j.id) + '" data-job-source="' + source + '">';
  html += '<div class="job-list-item-header">';
  html += '<span class="job-list-item-dot ' + dot + '"></span>';
  html += '<span class="job-list-item-type">' + escapeHtml(window.formatJobType(j.type)) + '</span>';
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
  var badge = jobsElement('runningBadge', HTMLElement);
  if (running.length > 0) { badge.textContent = String(running.length); badge.style.display = ''; }
  else { badge.style.display = 'none'; }
}

// Event delegation for job list clicks
function bindJobListClicks() {
  jobsElement('jobListContent', HTMLElement).addEventListener('click', function(e) {
    if (!(e.target instanceof Element)) return;
    var viewItem = e.target.closest('[data-view]');
    if (viewItem) {
      currentView = viewItem.getAttribute('data-view') || 'active';
      selectedJobId = null;
      selectedSource = null;
      if (sseSource) { sseSource.close(); sseSource = null; }
      renderMainPane();
      updateList();
      return;
    }
    var item = e.target.closest('[data-job-id]');
    if (item) selectJob(item.getAttribute('data-job-id') || '', item.getAttribute('data-job-source') || '');
  });
}
