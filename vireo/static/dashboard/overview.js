// The headline stats: overview counts, charts and attention cards.
// ES module.

import { escapeAttr, escapeHtml, safeFetch } from './shared.js';
import { renderClassification, renderDetection, renderFlags, renderHours, renderMonths, renderQuality, renderRatings, renderSpecies, renderTrendSummary } from './charts.js';
import { dashboardScopeIsActive, processScopeBlockedReason, scopedUrl } from './scope.js';
import { state } from './state.js';

export async function loadStats() {
  try {
    var data = /** @type {DashboardStats} */ (await safeFetch(scopedUrl('/api/stats'), {}, { toast: false }));
    state.dashboardStats = data;
    renderOverview(data);
    renderAttention(data.attention || {});
    renderSpecies(data.top_keywords || []);
    renderMonths(data.photos_by_month || []);
    renderRatings(data.rating_distribution || []);
    renderFlags(data.flag_distribution || []);
    renderClassification(data.prediction_status || [], data.classified_count || 0, data.total_photos || 0);
    renderDetection(data.detected_count || 0, data.total_photos || 0);
    renderQuality(data.quality_distribution || []);
    renderHours(data.photos_by_hour || []);
    renderTrendSummary(data.photos_by_month || []);
  } catch(e) {
    /** @type {HTMLElement} */ (document.getElementById('speciesChart')).textContent = 'Failed to load statistics.';
  }
}

/** @param {DashboardStats} d */
export function renderOverview(d) {
  /** @type {HTMLElement} */ (document.getElementById('photoCount')).textContent = (d.total_photos || 0).toLocaleString();
  /** @type {HTMLElement} */ (document.getElementById('folderCount')).textContent = (d.folder_count || 0).toLocaleString();
  /** @type {HTMLElement} */ (document.getElementById('keywordCount')).textContent = (d.keyword_count || 0).toLocaleString();
  /** @type {HTMLElement} */ (document.getElementById('pendingCount')).textContent = (d.pending_changes || 0).toLocaleString();

  var note = /** @type {HTMLElement} */ (document.getElementById('offlinePhotoNote'));
  var total = d.total_photos || 0;
  var accessible = typeof d.accessible_photos === 'number' ? d.accessible_photos : total;
  var missingFolders = d.missing_folder_count || 0;
  if (accessible < total && missingFolders > 0) {
    var offline = total - accessible;
    note.textContent = offline.toLocaleString() + ' in ' + missingFolders +
      ' offline folder' + (missingFolders === 1 ? '' : 's');
    note.style.display = 'block';
  } else {
    note.style.display = 'none';
  }
}

/**
 * One attention card; its action button names the action in
 * data-attention-action (boot.js runs it).
 *
 * @param {string} label @param {number} count @param {string} detail @param {string} actionLabel
 * @param {string} action @param {{done?: string, disabledReason?: string}=} options
 */
export function attentionCard(label, count, detail, actionLabel, action, options) {
  options = options || {};
  var done = count === 0;
  var displayCount = done ? '✓' : count.toLocaleString();
  var html = '<div class="attention-card ' + (done ? 'ok' : 'warn') + '">' +
    '<div class="attention-label">' + escapeHtml(label) + '</div>' +
    '<div class="attention-count">' + displayCount + '</div>' +
    '<div class="attention-detail">' + escapeHtml(done ? (options.done || 'Nothing needs attention.') : detail) + '</div>';
  if (!done && actionLabel && action) {
    if (options.disabledReason) {
      html += '<button class="attention-action" type="button" disabled ' +
        'style="cursor:not-allowed;opacity:0.55;" title="' +
        escapeAttr(options.disabledReason) + '">' +
        escapeHtml(actionLabel) + '</button>';
    } else {
      html += '<button class="attention-action" type="button" data-attention-action="' + escapeAttr(action) + '">' +
        escapeHtml(actionLabel) + ' →</button>';
    }
  }
  return html + '</div>';
}

/** @param {DashboardAttention} attention */
export function renderAttention(attention) {
  var previewDetail = attention.preview_enabled === false
    ? 'Preview generation is disabled in Settings.'
    : 'Accessible photos without a ' + (attention.preview_size || 1920) + 'px preview.';
  var previewDone = attention.preview_enabled === false
    ? 'Preview generation is disabled.' : 'Preview cache covers this scope.';
  var processBlocked = processScopeBlockedReason();
  var syncBlocked = dashboardScopeIsActive()
    ? 'Metadata sync runs workspace-wide. Reset the Dashboard scope to sync safely from this card.'
    : '';
  var duplicatesBlocked = dashboardScopeIsActive()
    ? 'Duplicate review is workspace-wide. Reset the Dashboard scope to review the same groups counted here.'
    : '';
  var html = '';
  html += attentionCard('Classification', attention.unclassified || 0,
    'Photos without a current classification.', 'Open Process', 'process-classify',
    {done: 'Every photo has a classification.', disabledReason: processBlocked});
  html += attentionCard('Map locations', attention.missing_location || 0,
    'Photos without embedded GPS or an assigned location.', 'Review photos',
    'browse-unlocated', {done: 'Every photo has a map location.'});
  html += attentionCard('Previews', attention.missing_previews || 0,
    previewDetail, 'Open Process', 'process-previews', {done: previewDone, disabledReason: processBlocked});
  html += attentionCard('Metadata sync', attention.pending_sync || 0,
    'Pending metadata changes waiting to be written to XMP.', 'Sync workspace',
    'sync', {done: 'XMP metadata is up to date.', disabledReason: syncBlocked});
  html += attentionCard('Duplicate groups', attention.duplicate_groups || 0,
    'Possible duplicate groups still needing a decision.', 'Review duplicates',
    'duplicates',
    {done: 'No unresolved duplicate groups.', disabledReason: duplicatesBlocked});
  /** @type {HTMLElement} */ (document.getElementById('attentionGrid')).innerHTML = html;
}
