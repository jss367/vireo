// The delegated click and double-click listeners for everything render.js
// draws: section headers (collapse), each section's Reject all button, the
// cards (select / open) and each card's unflag button; the rendered markup
// carries no inline handlers.
// ES module.

import { CATEGORY_ORDER } from './state.js';
import { toggleSection } from './render.js';
import { bulkReject, unflagMiss } from './flagging.js';
import { onCardClick, onCardDoubleClick } from './selection.js';

/** @param {string | undefined} value @returns {MissCategory | null} */
function categoryOf(value) {
  var cat = /** @type {MissCategory} */ (value);
  return CATEGORY_ORDER.includes(cat) ? cat : null;
}

/**
 * The inline handlers sat on the card, its unflag button, the section header
 * and its Reject all button; a button's handler stopped propagation, and the
 * card and header handlers ignored clicks inside a button. A disabled button
 * ran nothing. Resolving the innermost of those elements here keeps that.
 *
 * @param {MouseEvent} e
 */
function onMissesRootClick(e) {
  if (!(e.target instanceof Element)) return;
  var button = e.target.closest('button');
  if (button) {
    if (button.disabled) return;
    var buttonCard = button.closest(/** @type {'div'} */ ('.miss-card'));
    if (button.classList.contains('miss-card-unflag') && buttonCard) {
      var unflagCat = categoryOf(buttonCard.dataset.cat);
      var unflagId = parseInt(buttonCard.dataset.photoId || '', 10);
      if (unflagCat && !isNaN(unflagId)) unflagMiss(unflagCat, unflagId, e);
    } else if (button.classList.contains('misses-reject-btn')) {
      var rejectCat = categoryOf(button.dataset.cat);
      if (rejectCat) bulkReject(rejectCat, e);
    }
    return;
  }
  var card = e.target.closest(/** @type {'div'} */ ('.miss-card'));
  if (card) {
    var cat = categoryOf(card.dataset.cat);
    var idx = parseInt(card.dataset.idx || '', 10);
    var photoId = parseInt(card.dataset.photoId || '', 10);
    if (cat && !isNaN(idx) && !isNaN(photoId)) onCardClick(cat, idx, photoId, e);
    return;
  }
  var header = e.target.closest(/** @type {'div'} */ ('.misses-section-header'));
  if (header) {
    var headerCat = categoryOf(header.dataset.cat);
    if (headerCat) toggleSection(headerCat, e);
  }
}

/** @param {MouseEvent} e */
function onMissesRootDoubleClick(e) {
  if (!(e.target instanceof Element)) return;
  var card = e.target.closest(/** @type {'div'} */ ('.miss-card'));
  if (!card) return;
  var cat = categoryOf(card.dataset.cat);
  var idx = parseInt(card.dataset.idx || '', 10);
  if (cat && !isNaN(idx)) onCardDoubleClick(cat, idx, e);
}

export function bindMissCardEvents() {
  var root = document.getElementById('missesRoot');
  if (!root) return;
  root.addEventListener('click', onMissesRootClick);
  root.addEventListener('dblclick', onMissesRootDoubleClick);
}
