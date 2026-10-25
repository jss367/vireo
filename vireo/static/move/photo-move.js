// Photo Move: loading a folder's photos, the grid selection, and the move-photos job.

import { state } from './state.js';
import { showConfirm } from './confirm-modal.js';
import { refreshMoveVisibility } from './move-visibility.js';
import { escapeAttr, escapeHtml, safeFetch, showToast, thumbnailUrl } from './shared.js';

/* ---------- Photo Move ---------- */
export async function loadPhotosForFolder() {
  var sel = /** @type {HTMLSelectElement} */ (document.getElementById('photoFolderSelect'));
  var fid = parseInt(sel.value);
  var container = /** @type {HTMLElement} */ (document.getElementById('photoGridContainer'));
  var seq = ++state.photoLoadSeq;
  state.photoList = [];
  state.selectedPhotos.clear();
  state.lastSelectedIdx = -1;
  updatePhotoSelectionUI();

  if (!fid) {
    container.innerHTML = '<div class="empty-state">Select a folder to view photos</div>';
    return;
  }

  container.innerHTML = '<div class="loading">Loading photos...</div>';

  try {
    var page = 1;
    var total = null;
    /** @type {MovePhoto[]} */ var loadedPhotos = [];
    while (total === null || loadedPhotos.length < total) {
      /** @type {{photos?: MovePhoto[], total?: number}} */ var data = await safeFetch(
        '/api/photos?folder_id=' + encodeURIComponent(fid) +
        '&per_page=500&page=' + page
      );
      if (seq !== state.photoLoadSeq) return;

      var pagePhotos = data.photos || [];
      loadedPhotos = loadedPhotos.concat(pagePhotos);
      if (typeof data.total === 'number') total = data.total;

      if (pagePhotos.length < 500 || (total !== null && loadedPhotos.length >= total)) break;
      page += 1;
      var totalText = total !== null ? ' of ' + total.toLocaleString() : '';
      container.innerHTML = '<div class="loading">Loading photos... ' +
        loadedPhotos.length.toLocaleString() + totalText + '</div>';
    }

    state.photoList = loadedPhotos;
    if (state.photoList.length === 0) {
      container.innerHTML = '<div class="empty-state">No photos in this folder</div>';
      return;
    }
    renderPhotoGrid();
  } catch (e) {
    if (seq !== state.photoLoadSeq) return;
    container.innerHTML = '<div class="empty-state">Failed to load photos</div>';
  }
}

export function renderPhotoGrid() {
  var container = /** @type {HTMLElement} */ (document.getElementById('photoGridContainer'));
  var html = '<div class="photo-grid">';
  state.photoList.forEach(function(p, idx) {
    var selClass = state.selectedPhotos.has(p.id) ? ' selected' : '';
    var thumbUrl = thumbnailUrl(p);
    html += '<div class="grid-card' + selClass + '" data-id="' + p.id + '" data-idx="' + idx + '">';
    html += '<div class="grid-card-img-wrap"><img src="' + escapeAttr(thumbUrl) + '" loading="lazy" alt=""></div>';
    html += '<div class="grid-card-info"><div class="grid-card-name" title="' + escapeAttr(p.filename) + '">' + escapeHtml(p.filename) + '</div></div>';
    html += '</div>';
  });
  html += '</div>';
  container.innerHTML = html;
}

/** @param {MouseEvent} e @param {number} id @param {number} idx */
export function handlePhotoClick(e, id, idx) {
  e.preventDefault();
  if (e.shiftKey && state.lastSelectedIdx >= 0) {
    // Range select
    var start = Math.min(state.lastSelectedIdx, idx);
    var end = Math.max(state.lastSelectedIdx, idx);
    for (var i = start; i <= end; i++) {
      var photo = state.photoList[i]; if (photo) state.selectedPhotos.add(photo.id);
    }
  } else if (e.metaKey || e.ctrlKey) {
    // Toggle
    if (state.selectedPhotos.has(id)) {
      state.selectedPhotos.delete(id);
    } else {
      state.selectedPhotos.add(id);
    }
  } else {
    // Single select
    state.selectedPhotos.clear();
    state.selectedPhotos.add(id);
  }
  state.lastSelectedIdx = idx;

  // Update card highlights
  document.querySelectorAll(/** @type {'.grid-card'} */ ('#photoGridContainer .grid-card')).forEach(function(el) {
    var cardId = parseInt(el.getAttribute('data-id') || '');
    el.classList.toggle('selected', state.selectedPhotos.has(cardId));
  });

  updatePhotoSelectionUI();
}

export function clearPhotoSelection() {
  state.selectedPhotos.clear();
  state.lastSelectedIdx = -1;
  document.querySelectorAll(/** @type {'.grid-card'} */ ('#photoGridContainer .grid-card')).forEach(function(el) {
    el.classList.remove('selected');
  });
  updatePhotoSelectionUI();
}

export function updatePhotoSelectionUI() {
  var bar = /** @type {HTMLElement} */ (document.getElementById('photoSelectionBar'));
  var btn = /** @type {HTMLButtonElement} */ (document.getElementById('photoMoveBtn'));
  var count = state.selectedPhotos.size;

  if (count > 0) {
    bar.style.display = 'flex';
    /** @type {HTMLElement} */ (document.getElementById('photoSelCount')).textContent = count + ' photo' + (count !== 1 ? 's' : '') + ' selected';
  } else {
    bar.style.display = 'none';
  }

  var dest = /** @type {HTMLInputElement} */ (document.getElementById('photoDestInput')).value.trim();
  btn.disabled = !(count > 0 && dest && state.moveVisibilityReady);
  refreshMoveVisibility(Array.from(state.selectedPhotos)).catch(function() {});
}

export async function startPhotoMove() {
  var ids = Array.from(state.selectedPhotos);
  var dest = /** @type {HTMLInputElement} */ (document.getElementById('photoDestInput')).value.trim();
  if (ids.length === 0 || !dest || !state.moveVisibilityReady) return;
  var names;
  try { names = await refreshMoveVisibility(ids); } catch (e) { return; }
  var keepVisible = /** @type {HTMLInputElement} */ (document.getElementById('moveKeepVisible')).checked;
  var visibilityText = names.length ? ' Other workspaces: ' + names.join(', ') +
    (keepVisible ? '. Keep moved photos visible there.' : '. Allow removal of their visibility there.') : '';

  showConfirm(
    'Move Photos',
    'Move ' + ids.length + ' photo' + (ids.length !== 1 ? 's' : '') + ' to "' + dest + '"? Companion files will be included automatically.' + visibilityText,
    function() {
      executePhotoMove(ids, dest, keepVisible);
    }
  );
}

/** @param {number[]} photoIds @param {string} destination @param {boolean} keepVisible */
export async function executePhotoMove(photoIds, destination, keepVisible) {
  try {
    var resp = await safeFetch('/api/jobs/move-photos', {
      method: 'POST',
      headers: {'Content-Type': 'application/json'},
      body: JSON.stringify({photo_ids: photoIds, destination: destination, keep_visible: keepVisible}),
    });
    showToast('Move started (job ' + resp.job_id + ') — track progress in the bottom panel', 'info');
    // Clear selection after starting
    clearPhotoSelection();
  } catch (e) {
    // safeFetch already shows error toast
  }
}
