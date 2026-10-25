// The lightbox: advancing after a reject, the reject undo bar, and the
// highlight controls shown beside the open photo.
// ES module.

import { closeLightbox, escapeAttr, lightboxPhotoList, lightboxRequestedPhotoId, openLightbox, safeFetch, showToast } from './shared.js';
import { loadHighlights } from './loading.js';
import { findHighlightPhoto } from './ordering.js';
import { addSpeciesHighlight, confirmPhotoIds, moveSpeciesHighlight, photoCurrentLabel, photoSpeciesName, removeSpeciesHighlight, setSpeciesRepresentative, showSpeciesModal } from './species.js';
import { state } from './state.js';

/** @param {number} photoId */
export function lightboxIsOpenOnPhoto(photoId) {
  var overlay = /** @type {HTMLElement} */ (document.getElementById('lightboxOverlay'));
  return !!(
    overlay &&
    overlay.classList.contains('active') &&
    lightboxRequestedPhotoId() === photoId
  );
}

/** @param {number} photoId */
export function advanceHighlightLightboxAfterReject(photoId) {
  if (!lightboxIsOpenOnPhoto(photoId)) return;
  var photoList = lightboxPhotoList();
  if (!Array.isArray(photoList) || !photoList.length) return;

  var idx = photoList.findIndex(function(p) { return p.id === photoId; });
  if (idx < 0) return;
  var nextList = photoList.filter(function(p) {
    return p.id !== photoId && p.flag !== 'rejected';
  });
  if (!nextList.length) {
    closeLightbox();
    return;
  }
  var nextIdx = Math.min(idx, nextList.length - 1);
  var next = /** @type {HighlightsLightboxPhoto} */ (nextList[nextIdx]);
  openLightbox(next.id, next.filename || '', nextList);
}

/** @param {HighlightsPhoto} photo @param {string | undefined} previousFlag */
export function showHighlightUndo(photo, previousFlag) {
  var undo = state.lastHighlightReject = {
    photoId: photo.id,
    filename: photo.filename || 'photo',
    previousFlag: previousFlag || 'none',
  };
  var bar = /** @type {HTMLElement} */ (document.getElementById('highlightUndo'));
  var text = /** @type {HTMLElement} */ (document.getElementById('highlightUndoText'));
  var btn = /** @type {HTMLButtonElement} */ (document.getElementById('highlightUndoBtn'));
  if (!bar || !text || !btn) return;
  text.textContent = 'Rejected ' + undo.filename + '.';
  btn.disabled = false;
  bar.classList.add('open');
}

export function hideHighlightUndo() {
  state.lastHighlightReject = null;
  var bar = /** @type {HTMLElement} */ (document.getElementById('highlightUndo'));
  if (bar) bar.classList.remove('open');
}

export async function undoLastHighlightReject() {
  if (!state.lastHighlightReject) return;
  var undo = state.lastHighlightReject;
  var btn = /** @type {HTMLButtonElement} */ (document.getElementById('highlightUndoBtn'));
  if (btn) btn.disabled = true;
  try {
    await safeFetch('/api/photos/' + undo.photoId + '/flag', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ flag: undo.previousFlag }),
    });
    state.rejectedFromGrid.delete(undo.photoId);
    delete state.rejectedAtLoadSeq[undo.photoId];
    hideHighlightUndo();
    await loadHighlights();
    updateHighlightsLightboxControls(state.activeLightboxPhotoId);
    if (typeof showToast === 'function') showToast('Reject undone', 'success');
  } finally {
    if (btn) btn.disabled = false;
  }
}

export function ensureHighlightsLightboxPanel() {
  var actions = /** @type {HTMLElement} */ (document.getElementById('lightboxActions'));
  if (!actions) return null;
  var panel = /** @type {HTMLElement} */ (document.getElementById('highlightLightboxPanel'));
  if (!panel) {
    panel = document.createElement('div');
    panel.id = 'highlightLightboxPanel';
    panel.className = 'highlight-lb-panel';
    actions.insertBefore(panel, actions.firstChild);
  }
  return panel;
}

/** @param {number | null | undefined} photoId */
export function updateHighlightsLightboxControls(photoId) {
  var panel = ensureHighlightsLightboxPanel();
  if (!panel) return;
  var photo = findHighlightPhoto(photoId);
  if (!photo && !state.lastHighlightReject) {
    panel.style.display = 'none';
    panel.innerHTML = '';
    return;
  }
  panel.style.display = '';
  var html = photo
    ? '<span>' + escapeAttr(photoCurrentLabel(photo)) + '</span>'
    : '<span>Highlight photo removed</span>';
  if (photo) {
    var highlightSpecies = photoSpeciesName(photo);
    if (highlightSpecies) {
      if (photo.is_highlighted) {
        html += '<button class="primary" data-lb-action="remove-highlight">Highlighted</button>';
        html += '<button data-lb-action="highlight-up">Move Up</button>';
        html += '<button data-lb-action="highlight-down">Move Down</button>';
      } else {
        html += '<button data-lb-action="add-highlight">Add to Highlights</button>';
      }
    }
    if (photo.has_accepted_species && photo.species) {
      html += '<button data-lb-action="set-representative"'
        + (photo.is_species_representative ? ' class="primary"' : '') + '>'
        + (photo.is_species_representative ? 'Representative' : 'Set Representative') + '</button>';
    }
    if (photo.is_confirmable_prediction) {
      html += '<button class="primary" data-lb-action="confirm">Confirm</button>';
    }
    html += '<button data-lb-action="change">' + (photo.is_confirmable_prediction || photo.has_accepted_species ? 'Change' : 'Set species') + '</button>';
  }
  if (state.lastHighlightReject) {
    html += '<button class="primary" data-lb-action="undo-reject">Undo Reject</button>';
  }
  panel.innerHTML = html;
  panel.querySelectorAll('button').forEach(function(btn) {
    btn.addEventListener('click', function(event) {
      event.stopPropagation();
      var action = btn.getAttribute('data-lb-action');
      if (action === 'undo-reject') {
        undoLastHighlightReject();
      } else if (!photo) {
        return;
      } else if (action === 'confirm') {
        confirmPhotoIds([photo.id], photo.bucket_species || photoSpeciesName(photo), btn);
      } else if (action === 'add-highlight') {
        addSpeciesHighlight(photoSpeciesName(photo), photo.id, btn);
      } else if (action === 'remove-highlight') {
        removeSpeciesHighlight(photoSpeciesName(photo), photo.id, btn);
      } else if (action === 'highlight-up') {
        moveSpeciesHighlight(photoSpeciesName(photo), photo.id, 'up', btn);
      } else if (action === 'highlight-down') {
        moveSpeciesHighlight(photoSpeciesName(photo), photo.id, 'down', btn);
      } else if (action === 'set-representative') {
        setSpeciesRepresentative(photo.species, photo.id, btn);
      } else {
        var modalTitle = (photo.is_confirmable_prediction || photo.has_accepted_species)
          ? 'Change species'
          : 'Set species';
        showSpeciesModal(
          modalTitle, photo.filename || '', [photo.id], photoSpeciesName(photo),
          photo.bucket_species || null
        );
      }
    });
  });
}
