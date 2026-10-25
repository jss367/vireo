// Human review notes never feed the exact-byte resolver or change photo flags.
import { duplicatesEl, state } from './state.js';
import { escapeHtml, revealLabel, safeFetch, showToast, thumbnailUrl } from './shared.js';
import { renderSameShotResults } from './same-shot.js';

/** @param {SameShotGroup} group @param {boolean=} includeCompare */
export function reviewControls(group, includeCompare = true) {
  var disabled = state.sameShotReviewPending.has(group.review_key) ? ' disabled' : '';
  var key = ' data-same-shot-key="' + group.review_key + '"';
  var html = includeCompare ? '<button type="button" class="btn-secondary" data-same-shot-compare' + key + '>Compare images</button> ' : '';
  if (group.review_decision) {
    html += '<span class="same-shot-review-note">' + (group.review_decision === 'keep_all' ? 'Keep all copies' : 'Reviewed') +
      ' · ' + escapeHtml(group.reviewed_at || '') + ' UTC</span> ' +
      '<button type="button" class="btn-secondary" data-same-shot-decision="reopen"' + key + disabled + '>Reopen review</button>';
  } else {
    html += '<button type="button" class="btn-secondary" title="Record inspection and hide this group from pending review" data-same-shot-decision="reviewed"' + key + disabled + '>Mark reviewed</button> ' +
      '<button type="button" class="btn-secondary" title="Record your choice to retain every version and hide this group from pending review" data-same-shot-decision="keep_all"' + key + disabled + '>Keep all copies</button>';
  }
  return html + (disabled ? '<span role="status"> Saving review…</span>' : '');
}

/** @returns {HTMLDialogElement} */
function comparisonDialog() {
  return /** @type {HTMLDialogElement} */ (document.getElementById('sameShotCompare'));
}

export function closeSameShotComparison() {
  comparisonDialog().close();
  state.sameShotComparison = null;
}

/** @param {SameShotGroup} group */
export function openSameShotComparison(group) {
  state.sameShotComparison = group;
  var options = group.photos.map(function(photo, index) {
    var parts = (photo.path || '').split(/[\\/]/);
    var label = photo.filename + ' · ' + (photo.timestamp || 'Unknown date') + ' · ' + (parts[parts.length - 2] || '');
    return '<option value="' + index + '">' + escapeHtml(label) + '</option>';
  }).join('');
  var sides = ['Left', 'Right'].map(function(side, index) {
    return '<div><label>' + side + ' photo <select data-same-shot-side="' + index + '">' + options +
      '</select></label><div id="sameShotSide' + index + '" class="same-shot-compare-photo"></div></div>';
  }).join('');
  duplicatesEl('sameShotCompareBody').innerHTML = '<div class="same-shot-compare-grid">' + sides + '</div>';
  var selects = comparisonDialog().querySelectorAll('select');
  if (selects[1]) selects[1].value = '1';
  selects.forEach(function(select) { select.addEventListener('change', renderComparisonImages); });
  renderComparisonImages();
  duplicatesEl('sameShotCompareActions').innerHTML = reviewControls(group, false);
  duplicatesEl('sameShotCompareError').textContent = '';
  if (!comparisonDialog().open) comparisonDialog().showModal();
}

export function renderComparisonImages() {
  var group = state.sameShotComparison;
  if (!group) return;
  var photos = group.photos;
  comparisonDialog().querySelectorAll('select').forEach(function(select, side) {
    var photo = photos[Number(select.value)];
    if (!photo) return;
    var url = thumbnailUrl(photo).replace(/^\/thumbnails\//, '/thumbnails/duplicate/');
    duplicatesEl('sameShotSide' + side).innerHTML = '<img src="' + escapeHtml(url) + '" alt="' +
      escapeHtml(photo.filename || '') + '"><p class="same-shot-image-status" role="status">Loading cached thumbnail…</p>' +
      '<p>Captured: ' + escapeHtml((photo.timestamp || 'Unknown').replace('T', ' ')) + '</p>' +
      '<p class="path">' + escapeHtml(photo.path || '') + '</p>' +
      '<button type="button" class="btn-secondary" data-same-shot-reveal="' + photo.id + '">' + escapeHtml(revealLabel()) + '</button>';
    var image = /** @type {HTMLImageElement} */ (duplicatesEl('sameShotSide' + side).querySelector('img'));
    var status = /** @type {HTMLElement} */ (image.nextElementSibling);
    image.addEventListener('load', function() { status.textContent = 'Cached thumbnail'; });
    image.addEventListener('error', function() { status.textContent = 'Thumbnail unavailable. Reveal the original to inspect it.'; image.hidden = true; });
  });
}

/** @param {SameShotGroup} group @param {'reviewed' | 'keep_all' | null} decision */
export async function saveSameShotReview(group, decision) {
  var snapshot = state.lastScanResult;
  if (!snapshot || !snapshot.same_shot_groups?.some(function(candidate) { return candidate.review_key === group.review_key; }) ||
      state.sameShotReviewPending.has(group.review_key) || state.scanInProgress) return;
  state.sameShotReviewPending.add(group.review_key);
  renderSameShotResults(snapshot);
  if (state.sameShotComparison) duplicatesEl('sameShotCompareActions').innerHTML = reviewControls(group, false);
  duplicatesEl('sameShotCompareError').textContent = '';
  try {
    var data = await safeFetch('/api/duplicates/same-shot/review', {
      method: 'POST', headers: {'Content-Type': 'application/json'},
      body: JSON.stringify({photo_ids: group.photos.map(function(photo) { return photo.id; }), review_key: group.review_key, decision: decision}),
    }, {toast: false});
    var current = state.lastScanResult?.same_shot_groups?.find(function(candidate) { return candidate.review_key === group.review_key; });
    if (current) Object.assign(current, data.group);
    if (state.sameShotComparison?.review_key === group.review_key) closeSameShotComparison();
    showToast(decision ? 'Review saved for the library. All copies remain unchanged.' : 'Review reopened.');
  } catch (error) {
    var message = 'Could not save review: ' + (error instanceof Error ? error.message : String(error));
    if (state.sameShotComparison?.review_key === group.review_key) duplicatesEl('sameShotCompareError').textContent = message;
    showToast(message, 'error');
  } finally {
    state.sameShotReviewPending.delete(group.review_key);
    if (state.lastScanResult && !state.scanInProgress) renderSameShotResults(state.lastScanResult);
    if (state.sameShotComparison?.review_key === group.review_key) duplicatesEl('sameShotCompareActions').innerHTML = reviewControls(group, false);
  }
}

/** @param {Element} target @returns {boolean} */
export function handleSameShotReviewClick(target) {
  if (target.closest('[data-same-shot-close]')) { closeSameShotComparison(); return true; }
  if (target.closest('[data-same-shot-toggle-reviewed]') && state.lastScanResult && !state.scanInProgress) {
    state.sameShotShowReviewed = !state.sameShotShowReviewed;
    state.sameShotVisibleLimit = 50;
    renderSameShotResults(state.lastScanResult);
    return true;
  }
  var control = target.closest(/** @type {'button'} */ ('button[data-same-shot-key]'));
  if (!control) return false;
  if (control.disabled || state.scanInProgress) return true;
  var group = state.lastScanResult?.same_shot_groups?.find(function(candidate) { return candidate.review_key === control?.dataset.sameShotKey; });
  if (!group) return true;
  if (control.hasAttribute('data-same-shot-compare')) openSameShotComparison(group);
  else saveSameShotReview(group, control.dataset.sameShotDecision === 'reopen' ? null :
    /** @type {'reviewed' | 'keep_all'} */ (control.dataset.sameShotDecision));
  return true;
}
