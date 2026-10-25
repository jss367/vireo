// Camera-exposure candidates never enter exact-byte proposals or cleanup actions.

import { duplicatesEl, state } from './state.js';
import { escapeHtml, formatBytes, revealLabel, thumbnailUrl } from './shared.js';

/** @param {DuplicateScan} result */
export function renderSameShotResults(result) {
  var panel = duplicatesEl('sameShotResults');
  if (!result.same_shot_scanned) {
    panel.innerHTML = result.catalog_cleanup ? '' :
      '<p class="section-sub">Scan again to check for possible same-shot copies after metadata changes.</p>';
    return;
  }
  var all = result.same_shot_groups || [];
  var groups = all.filter(function(group) {
    return !state.filterMatchedIds || group.photos.some(function(photo) { return state.filterMatchedIds?.has(photo.id); });
  });
  var shown = groups.slice(0, state.sameShotVisibleLimit);
  var html = '<div class="section-header"><div><h2>Possible same-shot copies</h2>' +
    '<p class="section-sub">Showing ' + shown.length + ' of ' + groups.length + ' matching group' + (groups.length === 1 ? '' : 's') +
    '. Camera body serial and shutter count match; catalog file hashes differ. ' +
    'Review the images, dates and edits before deciding what to keep. ' +
    'These candidates are never automatically rejected or included in duplicate cleanup.</p>' +
    '<p class="section-sub">Checked ' + (result.same_shot_eligible_photos || 0).toLocaleString() +
    ' non-rejected photos with saved file hashes, camera identity and shutter count. ' +
    'Photos without these fields are outside this check. Photo filters apply here; filename pattern categories apply only to exact-byte groups.</p></div></div>';
  if (!groups.length) html += '<p class="no-results">' + (all.length
    ? 'No possible same-shot groups match the active photo filters.'
    : 'No possible same-shot copies found within this check’s coverage.') + '</p>';
  shown.forEach(function(group) { html += renderSameShotGroup(group); });
  if (shown.length < groups.length) html += '<button type="button" class="btn-secondary" data-same-shot-more>' +
    'Show ' + Math.min(50, groups.length - shown.length) + ' more groups</button>';
  panel.innerHTML = html;
}

/** @param {SameShotGroup} group */
export function renderSameShotGroup(group) {
  var evidence = group.visual_evidence === 'matching'
    ? 'Stored visual hashes also match. This supports the match; it does not prove identical image data.'
    : group.visual_evidence === 'different'
      ? 'Stored visual hashes differ. These may be different images or edited versions; inspect carefully.'
      : 'Visual comparison is unavailable for one or more photos. Camera metadata alone is not proof.';
  return '<div class="dup-group same-shot-group"><div class="dup-group-header">' +
    '<b>' + escapeHtml(group.camera) + ' · Body serial ' + escapeHtml(group.serial_number) +
    ' · Shutter count ' + group.shutter_count + '</b></div>' +
    '<p class="section-sub">' + evidence + ' Paths and metadata are from the catalog; file availability has not been checked.</p>' +
    '<div class="dup-row">' + group.photos.map(renderSameShotCard).join('') + '</div></div>';
}

/** @param {SameShotPhoto} photo */
export function renderSameShotCard(photo) {
  var thumb = thumbnailUrl(photo)
    .replace(/^\/thumbnails\//, '/thumbnails/duplicate/');
  var workspaces = photo.workspaces && photo.workspaces.length ? 'In ' + photo.workspaces.join(', ') : 'Not in any workspace';
  return '<div class="dup-card" data-photo-id="' + photo.id + '">' +
    '<img class="thumb" loading="lazy" decoding="async" width="180" height="135" src="' + escapeHtml(thumb) +
    '" alt="' + escapeHtml(photo.filename || '') + '">' +
    '<span class="badge">REVIEW</span>' +
    '<div class="filename">' + escapeHtml(photo.filename || '') + '</div>' +
    '<div class="same-shot-capture-time">Captured<br><b>' + escapeHtml((photo.timestamp || 'Unknown').replace('T', ' ')) + '</b></div>' +
    '<div class="path">' + escapeHtml(photo.path || '') + '</div>' +
    '<div class="workspaces">' + escapeHtml(workspaces) + '</div>' +
    '<div class="meta">' + formatBytes(photo.file_size || 0) +
    (photo.rating ? ' · ' + photo.rating + '★' : '') + '</div>' +
    '<button type="button" class="keep-btn" data-same-shot-reveal="' + photo.id + '">' +
    escapeHtml(revealLabel()) + '</button></div>';
}
