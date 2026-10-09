// Duplicates page: one copy's card in a duplicate group. Classic script
// sharing the page's globals (escapeHtml, formatBytes, _filterMatchedIds,
// trashOneLoserFile), which duplicates.html defines after loading this; they
// are only read when a card renders.

function renderCard(photo, isWinner, reason, isResolvedLoser, winnerMissingProtect, winnerOfflineProtect) {
  var cls = isWinner ? 'winner' : 'loser';
  // A copy on an offline volume is unknown, not missing — don't apply
  // the ``missing`` styling that renders it as gone.
  var missing = photo.exists === false && !photo.volume_offline;
  if (missing) cls += ' missing';
  var badge = isWinner ? 'KEEP' : (isResolvedLoser ? 'REJECTED' : 'WILL REJECT');
  // The scan is library-wide, so a copy may sit outside the active workspace,
  // where /thumbnails/ 404s. /thumbnails/duplicate/ serves any group member.
  var thumbUrl = photo.id != null
    ? (window.vireoThumbnailUrl ? window.vireoThumbnailUrl(photo) : '/thumbnails/' + photo.id + '.jpg')
        .replace(/^\/thumbnails\//, '/thumbnails/duplicate/')
    : null;
  var html = '<div class="dup-card ' + cls + '" data-photo-id="' +
             (photo.id != null ? photo.id : '') + '">';
  if (thumbUrl) {
    html += '<img class="thumb" src="' + escapeHtml(thumbUrl) +
            '" alt="' + escapeHtml(photo.filename || '') +
            '" onerror="this.outerHTML=\'<div class=\\\'thumb-placeholder\\\'>No thumbnail</div>\'">';
  } else {
    html += '<div class="thumb-placeholder">No thumbnail</div>';
  }
  html += '<span class="badge">' + badge + '</span>';
  if (_filterMatchedIds && photo.id != null && _filterMatchedIds.has(photo.id)) {
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
  if (isResolvedLoser && photo.id != null && !missing && !winnerMissingProtect) {
    html += '<button type="button" class="trash-btn" onclick="trashOneLoserFile(' +
            photo.id + ')">Move file to Trash</button>';
    html += '<div class="trash-status" data-trash-status></div>';
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

async function revealDuplicatePhoto(photoId) {
  try {
    var data = await safeFetch('/api/files/reveal', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ photo_id: photoId, scope: 'duplicates' }),
    }, { toast: false });
    showRevealFeedback(data);
  } catch (err) {
    showToast('Reveal failed: ' + (err.message || 'request failed'), 'error');
  }
}

// Delegate so restored results, fresh scans, and filter re-renders all work.
document.addEventListener('contextmenu', function(e) {
  var card = e.target.closest('.dup-card[data-photo-id]');
  var results = document.getElementById('results');
  if (!card || !results || !results.contains(card)) return;
  var photoId = Number(card.dataset.photoId);
  if (!Number.isInteger(photoId) || photoId <= 0) return;
  e.preventDefault();
  openContextMenu(e, [{
    label: window.VIREO_REVEAL_LABEL,
    onClick: function() { revealDuplicatePhoto(photoId); },
  }]);
});
