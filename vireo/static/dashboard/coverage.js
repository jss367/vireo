// Pipeline and attribute coverage, per scope and per folder.
// ES module.

import { safeFetch } from './shared.js';
import { drillAttrs } from './drill.js';
import { dashboardScopeIsActive, scopedUrl } from './scope.js';
import { state } from './state.js';

// Counts are split between Vireo outputs and facts that come from the image
// or the user's library decisions. The second group should not be read as
// pipeline completeness.
/** @type {DashboardCoverageField[]} */
export var PIPELINE_COVERAGE_FIELDS = [
  {key: 'file_hash',         label: 'File hash (SHA-256)'},
  {key: 'phash',             label: 'Perceptual hash'},
  {key: 'thumbnail',         label: 'Thumbnail'},
  {key: 'working_copy',      label: 'Working copy (JPG)'},
  {key: 'detected',          label: 'Subject detection'},
  {key: 'mask',              label: 'Subject mask'},
  {key: 'subject_sharpness', label: 'Subject sharpness'},
  {key: 'bg_sharpness',      label: 'Background sharpness'},
  {key: 'eye',               label: 'Eye detection'},
  {key: 'quality',           label: 'Quality score'},
  {key: 'dino_embedding',    label: 'DINO embedding'},
  {key: 'label_embedding',   label: 'Label embedding'},
  {key: 'classified',        label: 'Classification'},
];

/** @type {DashboardCoverageField[]} */
export var ATTRIBUTE_COVERAGE_FIELDS = [
  {key: 'timestamp',         label: 'EXIF timestamp'},
  {key: 'exif',              label: 'EXIF data'},
  {key: 'gps',               label: 'GPS coordinates'},
  {key: 'burst',             label: 'Camera burst marker'},
  {key: 'rating',            label: 'Rated (\u2605+)'},
];

export var COVERAGE_GROUPS = [
  {title: 'Pipeline Outputs', fields: PIPELINE_COVERAGE_FIELDS},
  {title: 'Image Attributes', fields: ATTRIBUTE_COVERAGE_FIELDS},
];
export async function loadCoverage() {
  var pipelineContainer = /** @type {HTMLElement} */ (document.getElementById('pipelineCoverageChart'));
  var attributeContainer = /** @type {HTMLElement} */ (document.getElementById('attributeCoverageChart'));
  try {
    var data = /** @type {{folders?: DashboardCoverageFolder[], overall?: DashboardCoverageCounts}} */ (
      await safeFetch(scopedUrl('/api/coverage'), {}, { toast: false }));
    state._coverageFolders = data.folders || [];
    var overall = data.overall || {total: 0};
    // When ``overall.total`` is 0, we need to know whether the workspace
    // is genuinely empty or whether all its folders are offline so we
    // can pick the right empty-state copy. ``/api/scan/status`` carries
    // that signal; only pay for the extra fetch on the empty branch.
    /** @type {DashboardOfflineContext | null} */
    var offlineCtx = null;
    if ((overall.total || 0) === 0 && !dashboardScopeIsActive()) {
      try {
        var status = /** @type {{photo_count?: number, missing_folder_count?: number, accessible_photo_count?: number}} */ (
          await safeFetch('/api/scan/status', {}, { toast: false }));
        if ((status.photo_count || 0) > 0 && (status.missing_folder_count || 0) > 0) {
          offlineCtx = {
            offline_photos: (status.photo_count || 0) - (status.accessible_photo_count || 0),
            missing_folders: status.missing_folder_count || 0,
          };
        }
      } catch (_) { /* fall through to default empty-state */ }
    }
    renderCoverageGroup(pipelineContainer, overall, PIPELINE_COVERAGE_FIELDS, offlineCtx);
    renderCoverageGroup(attributeContainer, overall, ATTRIBUTE_COVERAGE_FIELDS, offlineCtx);
  } catch(e) {
    pipelineContainer.innerHTML = '<span style="color:var(--danger);font-size:12px;">Failed to load coverage.</span>';
    attributeContainer.innerHTML = '<span style="color:var(--danger);font-size:12px;">Failed to load coverage.</span>';
  }
}

/**
 * @param {HTMLElement} container @param {DashboardCoverageCounts} stats
 * @param {DashboardCoverageField[]} fields @param {DashboardOfflineContext | null} offlineCtx
 */
export function renderCoverageGroup(container, stats, fields, offlineCtx) {
  var total = stats.total || 0;
  if (total === 0) {
    container.innerHTML = offlineCtx
      ? '<span style="color:var(--text-ghost);font-size:13px;">'
        + offlineCtx.offline_photos.toLocaleString()
        + ' photos are in offline folders — mount the storage to see coverage.</span>'
      : '<span style="color:var(--text-ghost);font-size:13px;">No photos in this workspace.</span>';
    return;
  }
  var html = '';
  fields.forEach(function(f) {
    var count = stats[f.key] || 0;
    var pct = Math.round((count / total) * 100);
    var attrs = '';
    if (f.key === 'gps') attrs = drillAttrs({location_status: 'exif'}, 'Browse photos with embedded GPS');
    else if (f.key === 'rating') attrs = drillAttrs({rating_min: 1}, 'Browse rated photos');
    if (attrs) attrs = attrs.replace('bar-row dashboard-drill', 'bar-row cov-bar dashboard-drill');
    html += '<div' + (attrs || ' class="bar-row cov-bar"') + '>' +
      '<span class="bar-label">' + f.label + '</span>' +
      '<div class="bar-track"><div class="bar-fill" style="width:' + pct + '%"></div></div>' +
      '<span class="cov-pct">' + pct + '%</span>' +
      '<span class="cov-count">' + count.toLocaleString() + ' / ' + total.toLocaleString() + '</span>' +
    '</div>';
  });
  container.innerHTML = html;
}

/** @param {DashboardCoverageFolder} folder */
export function renderFolderCoverage(folder) {
  var total = folder.total || 0;
  if (total === 0) {
    return '<div style="padding:4px 0 6px 14px;font-size:11px;color:var(--text-ghost);">No photos in this folder.</div>';
  }
  var html = '';
  COVERAGE_GROUPS.forEach(function(group) {
    html += '<div class="folder-cov-title">' + group.title + '</div>';
    html += '<div class="folder-cov-grid">';
    group.fields.forEach(function(f) {
      var count = /** @type {number | undefined} */ (folder[f.key]) || 0;
      var pct = Math.round((count / total) * 100);
      html += '<div class="bar-row cov-bar">' +
        '<span class="bar-label" title="' + f.label + ' \u2014 ' + count.toLocaleString() + ' / ' + total.toLocaleString() + '">' + f.label + '</span>' +
        '<div class="bar-track"><div class="bar-fill" style="width:' + pct + '%"></div></div>' +
        '<span class="cov-pct">' + pct + '%</span>' +
      '</div>';
    });
    html += '</div>';
  });
  return html;
}
