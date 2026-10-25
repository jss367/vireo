// Original-folder cleanup after a date-organized move: review and Trash.

import { jobsElement, state } from './state.js';
import { renderDetail, renderHistoryDetail } from './detail.js';
import { jobConfig, plural } from './format.js';
import { escapeAttr, escapeHtml, safeFetch } from './shared.js';

/** @param {JobsRow} job */ export function renderSourceCleanup(job) {
  var cfg = jobConfig(job);
  var result = job.result || {};
  if (job.type !== 'move-folder' || job.status !== 'completed' ||
      !cfg.folder_template || !cfg.source_path || !result.moved ||
      result.ok === false || (result.errors || []).length) return '';
  var review = state.sourceCleanupReviews[job.id];
  if (!review) {
    // Move results describe completion time. Refresh on each page load so
    // a later cleanup (including one in another tab) is not shown as pending.
    state.sourceCleanupReviews[job.id] = review = {checking: true};
    Promise.resolve().then(function() { requestSourceCleanup(job.id, false, true); });
  }
  var cleanup = review;
  var id = escapeAttr(job.id);
  var html = '<section class="move-cleanup" aria-label="Original folder cleanup">';
  if (cleanup.state === 'removed') {
    html += '<p>The original folder has been removed.</p>';
  } else {
    if (cleanup.checking) {
      html += '<p>Checking the original folder…</p>';
    } else if (typeof cleanup.file_count === 'number') {
      var n = cleanup.file_count;
      html += '<p>The original folder still contains ' + n.toLocaleString() + ' ' +
        (n > 0 && n === cleanup.xmp_count ? 'XMP metadata ' : '') + plural(n, 'file') + '.</p>';
    } else {
      html += '<p>Check the original folder for files left after the move.</p>';
    }
    if (cleanup.message) html += '<p role="status">' + escapeHtml(cleanup.message) + '</p>';
    if (cleanup.error) html += '<p role="alert">' + escapeHtml(cleanup.error) + '</p>';
    if (review && review.files && review.review_token) {
      if (review.files.length) {
        html += '<p>These files were not moved with the photos. Metadata files may contain editing settings.</p><ul>';
        review.files.forEach(function(file) {
          html += '<li>' + escapeHtml(file.name) + ' (' + file.size.toLocaleString() + ' bytes)</li>';
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

/** @param {string} jobId */ export function refreshCleanupDetail(jobId) {
  if (state.selectedJobId !== jobId) return;
  var job = state.activeJobs.concat(state.historyJobs).find(function(item) { return item.id === jobId; });
  if (job) {
    if (state.selectedSource === 'active') renderDetail(job);
    else renderHistoryDetail(job);
  }
}

/** @param {string} jobId @param {boolean} clean @param {boolean=} summary */ export async function requestSourceCleanup(jobId, clean, summary) {
  var prior = state.sourceCleanupReviews[jobId] || {};
  if (prior.busy || (clean && !prior.confirmed)) return;
  state.sourceCleanupReviews[jobId] = Object.assign({}, prior, {busy: true, error: null});
  refreshCleanupDetail(jobId);
  try {
    /** @type {JobsSourceCleanup} */ var data = await safeFetch('/api/jobs/' + encodeURIComponent(jobId) + '/source-cleanup' +
      (summary ? '?summary=1' : ''), clean ? {
      method: 'POST', headers: {'Content-Type': 'application/json'},
      body: JSON.stringify({confirm_trash: true, review_token: prior.review_token}),
    } : {});
    if (clean && data.state !== 'removed') {
      data.message = (data.trashed || 0) + ' files moved to Trash. Review the remaining files to retry.';
      if ((data.failures || []).length) {
        data.error = (data.failures || []).map(function(item) { return item.path + ': ' + item.error; }).join('; ');
      }
    }
    state.sourceCleanupReviews[jobId] = data;
  } catch (error) {
    // A failed or stale review never leaves an armed cleanup button.
    state.sourceCleanupReviews[jobId] = {error: error instanceof Error ? error.message : String(error)};
  }
  refreshCleanupDetail(jobId);
}

export function bindSourceCleanupConfirm() {
  jobsElement('jobDetailPane', HTMLElement).addEventListener('change', function(e) {
    if (!(e.target instanceof Element)) return;
    var checkbox = /** @type {HTMLInputElement | null} */ (e.target.closest('[data-confirm-source-cleanup]'));
    if (!checkbox) return;
    var id = checkbox.getAttribute('data-confirm-source-cleanup'); if (!id) return;
    var review = state.sourceCleanupReviews[id]; if (review) review.confirmed = checkbox.checked;
    var button = checkbox.closest('.move-cleanup')?.querySelector(/** @type {'button'} */ ('button[data-clean-source]'));
    if (button) button.disabled = !checkbox.checked;
  });
}
