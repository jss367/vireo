// Duplicates page: one copy's card in a duplicate group, and the cards'
// right-click menu (Reveal in the file manager). The card's Keep and Trash
// buttons carry data-dup-action attributes that events.js handles.

import { state } from './state.js';
import { duplicateFileTrashIsPending, duplicateRequestError } from './cleanup.js';
import {
  escapeHtml, formatBytes, openContextMenu, revealLabel, safeFetch, showRevealFeedback, showToast,
  thumbnailUrl,
} from './shared.js';

/** @param {DuplicatePhoto} photo @param {boolean} isWinner @param {string} [reason] @param {boolean} [isResolvedLoser] @param {boolean} [winnerMissingProtect] @param {boolean} [winnerOfflineProtect] */
export function renderCard(photo, isWinner, reason, isResolvedLoser, winnerMissingProtect, winnerOfflineProtect) {
  var cls = isWinner ? 'winner' : 'loser';
  // A copy on an offline volume is unknown, not missing — don't apply
  // the ``missing`` styling that renders it as gone.
  var missing = photo.exists === false && !photo.volume_offline;
  if (missing) cls += ' missing';
  var badge = isWinner ? 'KEEP' : (isResolvedLoser ? 'REJECTED' : 'WILL REJECT');
  // The scan is library-wide, so a copy may sit outside the active workspace,
  // where /thumbnails/ 404s. /thumbnails/duplicate/ serves any group member.
  var thumbUrl = photo.id != null
    ? thumbnailUrl(photo)
        .replace(/^\/thumbnails\//, '/thumbnails/duplicate/')
    : null;
  var html = '<div class="dup-card ' + cls + '" data-photo-id="' +
             (photo.id != null ? photo.id : '') + '">';
  if (thumbUrl) {
    html += '<div class="thumb-wrap"><div class="thumb-placeholder">Loading thumbnail…</div>' +
            '<img class="thumb" decoding="async" width="180" height="135" data-thumbnail-src="' + escapeHtml(thumbUrl) +
            '" alt="' + escapeHtml(photo.filename || '') +
            '" data-dup-thumb></div>';
  } else {
    html += '<div class="thumb-placeholder">No thumbnail</div>';
  }
  html += '<span class="badge">' + badge + '</span>';
  if (state.filterMatchedIds && photo.id != null && state.filterMatchedIds.has(photo.id)) {
    html += '<span class="filter-match-badge" title="This member matches the active filters">Matches filter</span>';
  }
  if (missing) html += '<span class="missing-tag" title="File is no longer at this path on disk">Missing on disk</span>';
  html += '<div class="filename">' + escapeHtml(photo.filename || '') + '</div>';
  html += '<div class="path">' + escapeHtml(photo.path || '') + '</div>';
  if (Array.isArray(photo.workspaces)) {
    html += '<div class="workspaces">' + (photo.workspaces.length
      ? 'In ' + escapeHtml(photo.workspaces.join(', '))
      : 'Not in any workspace') + '</div>';
  }
  var meta = [];
  if (photo.rating != null && photo.rating > 0) meta.push(photo.rating + '\u2605');
  if (photo.file_size != null) meta.push(formatBytes(photo.file_size));
  if (meta.length) html += '<div class="meta">' + meta.join(' \u00b7 ') + '</div>';
  if (reason) html += '<div class="reason">' + escapeHtml(reason) + '</div>';
  // Overrides the decision: this copy becomes the kept one. Not offered for a
  // copy that is missing or unreachable, which the server refuses anyway.
  if (isResolvedLoser && photo.id != null && !missing && !photo.volume_offline) {
    var busy = state.duplicateTrashActive || duplicateFileTrashIsPending(photo.id) || state.duplicateKeepPending.has(photo.id);
    html += '<button type="button" class="keep-btn" data-dup-action="keep-copy"' +
            (busy ? ' disabled' : '') + '>Keep this copy instead</button>';
  }
  if (isResolvedLoser && photo.id != null && !missing && !winnerMissingProtect) {
    var trashState = state.duplicateFileTrashState.get(photo.id);
    html += '<button type="button" class="trash-btn" data-dup-action="trash-copy"' +
            ((trashState && trashState.pending) || state.duplicateKeepPending.has(photo.id) ? ' disabled' : '') +
            '>Move file to Trash</button>';
    html += '<div class="trash-status" data-trash-status>' + escapeHtml(trashState ? trashState.message : '') + '</div>';
  } else if (isResolvedLoser && !missing && winnerMissingProtect) {
    // The kept-file is gone (or its volume is unreachable and we can't
    // tell), so this loser file may be the only remaining copy. Suppress
    // the trash button and explain why.
    var msg = winnerOfflineProtect
      ? 'Trash disabled \u2014 kept file is on an unreachable volume; reconnect it before removing copies.'
      : 'Trash disabled \u2014 kept file is missing, this may be the only copy.';
    html += '<div class="trash-status" data-trash-status>' + msg + '</div>';
  }
  html += '</div>';
  return html;
}

/** @param {number} photoId */
export async function revealDuplicatePhoto(photoId) {
  try {
    var data = await safeFetch('/api/files/reveal', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ photo_id: photoId, scope: 'duplicates' }),
    }, { toast: false });
    showRevealFeedback(data);
  } catch (caught) { const err = duplicateRequestError(caught);
    showToast('Reveal failed: ' + (err.message || 'request failed'), 'error');
  }
}

// Delegate so restored results, fresh scans, and filter re-renders all work.
export function bindDuplicateCardContextMenu() {
  document.addEventListener('contextmenu', onDuplicateCardContextMenu);
}

/** @param {MouseEvent} e */
function onDuplicateCardContextMenu(e) {
  if (!(e.target instanceof Element)) return;
  var card = e.target.closest(/** @type {'div'} */ ('.dup-card[data-photo-id]'));
  var results = /** @type {HTMLElement} */ (document.getElementById('results'));
  if (!card || !results || (!results.contains(card) && !card.closest('#sameShotResults'))) return;
  var photoId = Number(card.dataset.photoId);
  if (!Number.isInteger(photoId) || photoId <= 0) return;
  e.preventDefault();
  openContextMenu(e, [{
    label: revealLabel(),
    onClick: function() { revealDuplicatePhoto(photoId); },
  }]);
}
