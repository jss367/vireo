// Rendering the ranked batch: the status line, the focus-check crops, the
// ranked list, and the large preview of the inspected frame.
// ES module.

import { byId, state } from './state.js';
import { escapeAttr, escapeHtml } from './shared.js';
import {
  focusImageStyle, hasEyeKeypoint, previewUrl, rankLabel, scoreText, thumbnailUrl,
} from './helpers.js';

/** @param {string} message */
export function setStatus(message) {
  var status = byId('bestStatus');
  var content = byId('bestContent');
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
    img.src = thumbnailUrl(card);
  };
  state.inspectImgFallback = fallback;
  img.addEventListener('error', fallback);
  img.src = previewUrl(card.id, 1920);
  img.alt = card.filename || '';
}

/** @param {number} photoId */
export function inspectPhoto(photoId) {
  if (!state.bestBatchData || !state.bestBatchData.cards) return;
  var card = state.bestBatchData.cards.find(function(c) { return c.id === photoId; });
  if (!card) return;
  state.inspectedId = card.id;
  showInspectImage(card);

  var role = byId('inspectRole');
  role.className = 'role ' + (card.role || '');
  role.textContent = rankLabel(card);
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
  var hasEye = hasEyeKeypoint(card);
  return '<div class="focus-card ' + escapeAttr(card.role || '') + '" data-photo-id="' + card.id + '">' +
    '<div class="focus-crop ' + (hasEye ? 'eye' : 'subject') + '">' +
      '<img src="' + previewUrl(card.id, 1920) + '" alt="' + escapeAttr(card.filename || '') + '" loading="lazy" style="' + escapeAttr(focusImageStyle(card)) + '">' +
      (hasEye ? '<span class="crosshair"></span>' : '') +
    '</div>' +
    '<div class="focus-meta">' +
      '<div class="role ' + escapeAttr(card.role || '') + '">' + escapeHtml(rankLabel(card)) + '</div>' +
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
      '<div class="role ' + escapeAttr(card.role || '') + '">' + escapeHtml(rankLabel(card)) + '</div>' +
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

  byId('bestStatus').style.display = 'none';
  byId('bestContent').style.display = '';
  byId('bestSubtitle').textContent =
    data.count + ' photos' + (data.sequence_range ? ' · #' + data.sequence_range[0] + '-' + data.sequence_range[1] : '');

  byId('focusNote').textContent = cards.some(hasEyeKeypoint)
    ? 'Eye crops are centered on detected keypoints. Click a crop to inspect the full frame.'
    : 'No eye keypoints found for this batch; focus evidence falls back to subject/full-frame context.';

  byId('focusGrid').innerHTML = cards.map(focusCardHtml).join('');
  byId('rankList').innerHTML = cards.map(rankCardHtml).join('');

  /** @type {HTMLButtonElement} */ (byId('reviewBtn')).disabled = !data.photo_ids || data.photo_ids.length < 2;
  /** @type {HTMLButtonElement} */ (byId('flagBtn')).disabled = !data.best_photo_id;
  /** @type {HTMLButtonElement} */ (byId('rejectBtn')).disabled = !data.best_photo_id;
  inspectPhoto(best.id);
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
