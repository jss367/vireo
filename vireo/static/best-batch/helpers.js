// Pure helpers: the JSON request wrapper, image URLs, the role and score
// labels, the eye-crop placement, and reading the batch Browse handed over.
// ES module.

import { vireoPreviewUrl, vireoThumbnailUrl } from './shared.js';

/**
 * @param {string} url @param {RequestInit=} options
 * @returns {Promise<any>}
 */
export async function fetchJson(url, options) {
  var resp = await fetch(url, options || {});
  var data = null;
  try { data = await resp.json(); } catch (e) {}
  if (!resp.ok) {
    throw new Error((data && data.error) || ('Request failed: ' + resp.status));
  }
  return data;
}

/** @param {number} photoId @param {number=} size */
export function previewUrl(photoId, size) {
  return vireoPreviewUrl(photoId, size || 1920);
}

/** @param {BestBatchCard | number} photoOrId */
export function thumbnailUrl(photoOrId) {
  return vireoThumbnailUrl(photoOrId);
}

/** @param {string | undefined} role */
export function roleLabel(role) {
  if (role === 'best') return 'Best';
  if (role === 'alternate') return 'Alt';
  return role === 'review' ? 'Review' : 'Reject';
}

/** @param {BestBatchCard} card */
export function scoreText(card) {
  var parts = [];
  if (card.quality_pct != null) parts.push('Q ' + card.quality_pct);
  if (card.focus != null) parts.push((card.focus_basis === 'eye' ? 'Eye' : 'Subject') + ' ' + Math.round(card.focus * 100));
  if (card.sharpness != null) parts.push('Subject S ' + Math.round(card.sharpness));
  return parts.join(' · ') || 'No score';
}

/** @param {number} value @param {number} min @param {number} max */
function clamp(value, min, max) {
  return Math.max(min, Math.min(max, value));
}

/** @param {BestBatchCard} card */
export function hasEyeKeypoint(card) {
  return card.focus_basis === 'eye' && card.eye_x != null && card.eye_y != null;
}

/** Inline position that centers a 7.8x crop on the detected eye. @param {BestBatchCard} card */
export function focusImageStyle(card) {
  if (!hasEyeKeypoint(card)) {
    return '';
  }
  var zoom = 7.8;
  var width = Number(card.width) || 1;
  var height = Number(card.height) || width;
  var imageWidthPct = zoom * 100;
  var imageHeightPct = imageWidthPct * (height / width);
  var x = clamp(Number(card.eye_x), 0, 1);
  var y = clamp(Number(card.eye_y), 0, 1);
  var left = clamp(50 - x * imageWidthPct, 100 - imageWidthPct, 0);
  var top = clamp(50 - y * imageHeightPct, 100 - imageHeightPct, 0);
  return 'left:' + left.toFixed(2) + '%;top:' + top.toFixed(2) + '%;';
}

/** The photo ids Browse stored for this batch. @returns {number[]} */
export function selectedIdsFromStorage() {
  try {
    var raw = window.sessionStorage.getItem('vireo.bestBatchIds');
    var ids = raw ? JSON.parse(raw) : [];
    return Array.isArray(ids) ? ids.filter(function(id) { return Number.isInteger(id); }) : [];
  } catch (e) {
    return [];
  }
}

/** The photo the batch is built around: ?photo_id=, then Browse's seed, then the first id. @param {number[]} ids */
export function seedIdFromRequest(ids) {
  var params = new URLSearchParams(window.location.search);
  var queryId = Number(params.get('photo_id'));
  if (Number.isInteger(queryId) && queryId > 0) return queryId;
  try {
    var stored = Number(window.sessionStorage.getItem('vireo.bestBatchSeedId'));
    if (Number.isInteger(stored) && stored > 0) return stored;
  } catch (e) {}
  return ids.length ? ids[0] : null;
}
