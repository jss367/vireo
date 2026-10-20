// Rendering the ranked batch: the status line, the focus-check crops, the
// ranked list, and the large preview of the inspected frame.
// ES module.

import { renderFocus } from './focus.js';
import { renderSubject } from './subjects.js';
import { byId, state } from './state.js';
import { escapeAttr, escapeHtml } from './shared.js';
import {
  hasEyeKeypoint, previewUrl, roleLabel, scoreText, thumbnailUrl,
} from './helpers.js';

/** @param {string} message */
export function setStatus(message) {
  var status = byId('bestStatus');
  var content = byId('bestContent');
  state.bestBatchData = null;
  ['flagBtn', 'rejectBtn', 'reviewBtn', 'analyzeSubjectsBtn'].forEach(function(id) {
    /** @type {HTMLButtonElement} */ (byId(id)).disabled = true;
  });
  status.textContent = message;
  status.style.display = '';
  content.style.display = 'none';
}

/** Show the preview image, falling back to the thumbnail once if it fails. @param {BestBatchCard} card */
function showInspectImage(card) {
  var img = /** @type {HTMLImageElement} */ (byId('inspectImg'));
  if (state.inspectImgFallback) img.removeEventListener('error', state.inspectImgFallback);
  var fallback = function() {
    img.removeEventListener('error', fallback);
    state.inspectImgFallback = null;
    img.removeAttribute('src');
    byId('focusRasterStatus').textContent = 'Full frame unavailable; manual placement requires the upright source image.';
  };
  state.inspectImgFallback = fallback;
  img.addEventListener('error', fallback);
  img.src = '/photos/' + card.id + '/edit-preview?size=1920&recipe=%7B%7D';
  img.alt = card.filename || '';
}

/** @param {number} photoId */
export function inspectPhoto(photoId) {
  if (!state.bestBatchData || !state.bestBatchData.cards) return;
  var card = state.bestBatchData.cards.find(function(c) { return c.id === photoId; });
  if (!card) return;
  state.inspectedId = card.id;
  showInspectImage(card);
  renderSubject(card);
  renderFocus(card);

  var role = byId('inspectRole');
  role.className = 'role ' + (card.role || '');
  role.textContent = (card.rank == null ? '' : '#' + card.rank + ' ') + roleLabel(card.role);
  byId('inspectName').textContent = card.filename || '';
  byId('inspectScore').textContent = scoreText(card);
  byId('inspectReasons').innerHTML = (card.reasons || []).map(function(reason) {
    return '<span class="reason">' + escapeHtml(reason) + '</span>';
  }).join('');

  document.querySelectorAll('.focus-card,.rank-card').forEach(function(node) {
    var el = /** @type {HTMLElement} */ (node);
    el.classList.toggle('inspecting', Number(el.dataset.photoId) === photoId);
  });
}

/** @param {BestBatchCard} card */
function focusCardHtml(card) {
  var hasEye = Boolean(card.subject && card.focus_crop_url && hasEyeKeypoint(card));
  return '<div class="focus-card ' + escapeAttr(card.role || '') + '" data-photo-id="' + card.id + '">' +
    '<div class="focus-crop ' + 'native' + '">' +
      '<img src="' + escapeAttr(card.focus_crop_url || card.crop_url || previewUrl(card.id, 1920)) + '" alt="' + escapeAttr(card.filename || '') + '" loading="lazy" style="' + '' + '">' +
      (hasEye ? '<span class="crosshair"></span>' : '') +
    '</div>' +
    '<div class="focus-meta">' +
      '<div class="score">' + (card.focus_crop_url ? 'Native source window' : 'Context preview · inspect focus below') + '</div>' +
      '<div class="role ' + escapeAttr(card.role || '') + '">' + (card.rank == null ? '' : '#' + card.rank + ' ') + roleLabel(card.role) + '</div>' +
      '<div class="filename" title="' + escapeAttr(card.filename || '') + '">' + escapeHtml(card.filename || '') + '</div>' +
      '<div class="score">' + escapeHtml(scoreText(card)) + '</div>' +
    '</div>' +
  '</div>';
}

/** @param {BestBatchCard} card */
function rankCardHtml(card) {
  var reasons = (card.reasons || []).slice(0, 2).join(' · ');
  return '<div class="rank-card ' + escapeAttr(card.role || '') + '" data-photo-id="' + card.id + '">' +
    '<img src="' + thumbnailUrl(card) + '" alt="' + escapeAttr(card.filename || '') + '" loading="lazy">' +
    '<div class="rank-meta">' +
      '<div class="role ' + escapeAttr(card.role || '') + '">' + (card.rank == null ? '' : '#' + card.rank + ' ') + roleLabel(card.role) + '</div>' +
      '<div class="filename" title="' + escapeAttr(card.filename || '') + '">' + escapeHtml(card.filename || '') + '</div>' +
      '<div class="score">' + escapeHtml(scoreText(card)) + '</div>' +
      '<div class="score">' + escapeHtml(reasons) + '</div>' +
    '</div>' +
  '</div>';
}

/** @param {BestBatchResponse} data */
export function renderBatch(data) {
  state.bestBatchData = data;
  var cards = data.cards || [];
  var best = cards.find(function(c) { return c.id === data.best_photo_id; }) || cards[0];
  if (!best) {
    setStatus('No scored photos found in this batch.');
    return;
  }

  var notice = byId('comparisonNotice');
  notice.hidden = !data.comparison;
  var unresolved = cards.filter(function(c) { return c.evidence_status !== 'ready'; }).length;
  notice.textContent = data.unscored_reason || (unresolved ? unresolved + ' frames need a subject choice or analysis. Ranking covers the ready subjects.' :
    !data.best_photo_id && data.scored ? 'No frame passed the current scoring checks for a suggested best.' : 'Inspect the selected subject and full-frame context before deciding.');
  byId('bestStatus').style.display = 'none';
  byId('bestContent').style.display = '';
  byId('bestSubtitle').textContent =
    data.count + ' photos' + (data.comparison ? ' · Comparing ' + (data.comparison.requested_species || 'chosen subjects') + ' · ' + cards.filter(function(c) { return c.rank != null; }).length + ' ranked' : '') + (data.sequence_range ? ' · #' + data.sequence_range[0] + '-' + data.sequence_range[1] : '');

  byId('focusNote').textContent = 'Verify the model box in the full frame. Use manual placement when the suggested subject is wrong; source and scale are shown in the inspection panel.';

  byId('focusGrid').classList.toggle('comparison', !!data.comparison);
  byId('focusGrid').innerHTML = cards.map(focusCardHtml).join('');
  byId('rankList').innerHTML = cards.map(rankCardHtml).join('');

  /** @type {HTMLButtonElement} */ (byId('reviewBtn')).disabled = !!data.comparison || !data.photo_ids || data.photo_ids.length < 2;
  byId('reviewBtn').title = data.comparison ? 'Review the selected subjects with the comparison controls below.' : '';
  /** @type {HTMLButtonElement} */ (byId('analyzeSubjectsBtn')).disabled = !data.comparison;
  /** @type {HTMLButtonElement} */ (byId('flagBtn')).disabled = !data.best_photo_id;
  /** @type {HTMLButtonElement} */ (byId('rejectBtn')).disabled = !data.best_photo_id;
  var inspected = cards.find(function(c) { return c.id === state.inspectedId; });
  inspectPhoto(inspected ? inspected.id : best.id);
}

/** A click on a focus crop or ranked frame inspects it; delegated so re-rendering keeps it bound. */
export function bindCardClicks() {
  ['focusGrid', 'rankList'].forEach(function(id) {
    byId(id).addEventListener('click', function(event) {
      var card = event.target instanceof Element ? event.target.closest('[data-photo-id]') : null;
      if (card) inspectPhoto(Number(card.getAttribute('data-photo-id')));
    });
  });
}
