// Original-folder cleanup after a date-organized move: review and Trash.
// Classic page script; load boot.js after all definitions.

var sourceCleanupReviews = {};

function renderSourceCleanup(job) {
  var cfg = jobConfig(job);
  var result = job.result || {};
  if (job.type !== 'move-folder' || job.status !== 'completed' ||
      !cfg.folder_template || !cfg.source_path || !result.moved ||
      result.ok === false || (result.errors || []).length) return '';
  var review = sourceCleanupReviews[job.id];
  if (!review) {
    // Move results describe completion time. Refresh on each page load so
    // a later cleanup (including one in another tab) is not shown as pending.
    sourceCleanupReviews[job.id] = review = {checking: true};
    Promise.resolve().then(function() { requestSourceCleanup(job.id, false, true); });
  }
  var state = review;
  var id = escapeAttr(job.id);
  var html = '<section class="move-cleanup" aria-label="Original folder cleanup">';
  if (state.state === 'removed') {
    html += '<p>The original folder has been removed.</p>';
  } else {
    if (state.checking) {
      html += '<p>Checking the original folder…</p>';
    } else if (typeof state.file_count === 'number') {
      var n = state.file_count;
      html += '<p>The original folder still contains ' + n.toLocaleString() + ' ' +
        (n > 0 && n === state.xmp_count ? 'XMP metadata ' : '') + plural(n, 'file') + '.</p>';
    } else {
      html += '<p>Check the original folder for files left after the move.</p>';
    }
    if (state.message) html += '<p role="status">' + esc(state.message) + '</p>';
    if (state.error) html += '<p role="alert">' + esc(state.error) + '</p>';
    if (review && review.files && review.review_token) {
      if (review.files.length) {
        html += '<p>These files were not moved with the photos. Metadata files may contain editing settings.</p><ul>';
        review.files.forEach(function(file) {
          html += '<li>' + esc(file.name) + ' (' + file.size.toLocaleString() + ' bytes)</li>';
        });
        html += '</ul>';
      }
      if ((review.directories || []).length) {
        html += '<p>Empty subfolders will also be removed.</p>';
      }
      html += '<label><input type="checkbox" data-confirm-source-cleanup="' + id + '"' +
        (review.confirmed ? ' checked' : '') + (review.busy ? ' disabled' : '') + '> ' +
        'Move remaining files to Trash and remove the empty folder</label>';
      html += '<button class="btn-retry" data-clean-source="' + id + '"' +
        (!review.confirmed || review.busy ? ' disabled' : '') + '>' +
        (review.busy ? 'Moving files to Trash…' : 'Clean up original folder') + '</button> ';
    }
    html += '<button class="btn-retry" data-review-source="' + id + '"' +
      (review && review.busy ? ' disabled' : '') + '>Review remaining files</button>';
  }
  return html + '</section>';
}

function refreshCleanupDetail(jobId) {
  if (selectedJobId !== jobId) return;
  var job = activeJobs.concat(historyJobs).find(function(item) { return item.id === jobId; });
  if (job) {
    if (selectedSource === 'active') renderDetail(job);
    else renderHistoryDetail(job);
  }
}

async function requestSourceCleanup(jobId, clean, summary) {
  var prior = sourceCleanupReviews[jobId] || {};
  if (prior.busy || (clean && !prior.confirmed)) return;
  sourceCleanupReviews[jobId] = Object.assign({}, prior, {busy: true, error: null});
  refreshCleanupDetail(jobId);
  try {
    var data = await safeFetch('/api/jobs/' + encodeURIComponent(jobId) + '/source-cleanup' +
      (summary ? '?summary=1' : ''), clean ? {
      method: 'POST', headers: {'Content-Type': 'application/json'},
      body: JSON.stringify({confirm_trash: true, review_token: prior.review_token}),
    } : {});
    if (clean && data.state !== 'removed') {
      data.message = (data.trashed || 0) + ' files moved to Trash. Review the remaining files to retry.';
      if ((data.failures || []).length) {
        data.error = data.failures.map(function(item) { return item.path + ': ' + item.error; }).join('; ');
      }
    }
    sourceCleanupReviews[jobId] = data;
  } catch (error) {
    // A failed or stale review never leaves an armed cleanup button.
    sourceCleanupReviews[jobId] = {error: error.message};
  }
  refreshCleanupDetail(jobId);
}

function bindSourceCleanupConfirm() {
  document.getElementById('jobDetailPane').addEventListener('change', function(e) {
    var checkbox = e.target.closest('[data-confirm-source-cleanup]');
    if (!checkbox) return;
    var id = checkbox.getAttribute('data-confirm-source-cleanup');
    if (sourceCleanupReviews[id]) sourceCleanupReviews[id].confirmed = checkbox.checked;
    var button = checkbox.closest('.move-cleanup').querySelector('[data-clean-source]');
    if (button) button.disabled = !checkbox.checked;
  });
}
