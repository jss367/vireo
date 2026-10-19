// The pending-changes panel shown beside the lightbox while the review
// dialog is open, with per-photo discards.
// Classic script.

/** @param {number | null | undefined} photoId */
function _syncFindPhotoData(photoId) {
  if (!_syncPreviewData || !_syncPreviewData.photos) return null;
  return _syncPreviewData.photos.find(function(p) { return p.photo_id === photoId; }) || null;
}

/** @param {SyncPhoto | null} photo */
function _syncVisibleChangesFor(photo) {
  if (!photo) return [];
  var filters = _syncActiveFilters;
  if (!filters) return photo.changes.slice();
  return photo.changes.filter(function(c) { return /** @type {Set<string>} */ (filters).has(c.type); });
}

function _syncEnsureLightboxPanel() {
  if (_syncLightboxPanelEl && _syncLightboxPanelEl.parentNode) return _syncLightboxPanelEl;
  var overlay = /** @type {HTMLElement} */ (document.getElementById('lightboxOverlay'));
  if (!overlay) return null;
  var panel = document.createElement('div');
  panel.id = 'syncLightboxPanel';
  panel.style.cssText = 'position:absolute;top:56px;right:16px;max-width:340px;background:rgba(0,0,0,0.7);color:var(--text-primary);border:1px solid var(--border-secondary);border-radius:6px;padding:10px 12px;font-size:12px;z-index:10002;box-shadow:0 4px 16px rgba(0,0,0,0.5);';
  // Handle discard clicks on the panel itself: we stopPropagation() so the
  // lightbox overlay's onclick doesn't close the lightbox out from under the
  // user, which means a document-level delegate would never see these events.
  panel.addEventListener('click', function(e) {
    var target = e.target instanceof Element ? e.target : null;
    var changeBtn = target && target.closest('button[data-sync-discard-change]');
    if (changeBtn) {
      var changeId = parseInt(changeBtn.getAttribute('data-sync-discard-change') || '', 10);
      if (!isNaN(changeId)) _syncDiscardFromLightbox([changeId], vireoLightboxSession.requestedPhotoId());
    } else {
      var photoBtn = target && target.closest('button[data-sync-discard-photo]');
      if (photoBtn) {
        var pid = parseInt(photoBtn.getAttribute('data-sync-discard-photo') || '', 10);
        var photo = _syncFindPhotoData(pid);
        if (photo) {
          var ids = _syncVisibleChangesFor(photo).map(function(c) { return c.id; });
          _syncDiscardFromLightbox(ids, pid);
        }
      }
    }
    e.stopPropagation();
  });
  overlay.appendChild(panel);
  _syncLightboxPanelEl = panel;
  return panel;
}

function _syncDestroyLightboxPanel() {
  if (_syncLightboxPanelEl && _syncLightboxPanelEl.parentNode) {
    _syncLightboxPanelEl.parentNode.removeChild(_syncLightboxPanelEl);
  }
  _syncLightboxPanelEl = null;
}

/** @param {number | null | undefined} photoId */
function _syncRenderLightboxPanel(photoId) {
  if (!_syncModalOpen) { _syncDestroyLightboxPanel(); return; }
  var photo = _syncFindPhotoData(photoId);
  var visible = _syncVisibleChangesFor(photo);
  if (!photo || visible.length === 0) {
    _syncDestroyLightboxPanel();
    return;
  }
  var panel = _syncEnsureLightboxPanel();
  if (!panel) return;
  var actionsDisabled = _syncPreviewComplete ? '' : ' disabled';
  var html = '<div style="font-weight:600;margin-bottom:6px;color:var(--accent);">Pending changes</div>';
  visible.forEach(function(c) {
    var style = _syncChangeStyle(c);
    var presentation = _syncChangePresentation(c, photo, _syncActiveFilters);
    html += '<div style="display:flex;align-items:flex-start;gap:6px;padding:5px 0;">' +
      '<span style="color:' + style.color + ';font-weight:700;width:12px;text-align:center;">' + style.icon + '</span>' +
      '<div style="flex:1;min-width:0;">' +
        '<div style="color:var(--text-primary);font-weight:600;">' + escapeHtml(presentation.field) + '</div>' +
        '<div style="margin-top:2px;color:var(--text-dim);">' + escapeHtml(presentation.before) + ' \u2192 <span style="color:var(--text-primary);">' + escapeHtml(presentation.after) + '</span></div>' +
        (presentation.after_detail ? '<div style="margin-top:2px;color:var(--text-ghost);font-size:10px;">' + escapeHtml(presentation.after_detail) + '</div>' : '') +
      '</div>' +
      '<button class="sync-lightbox-action" data-sync-discard-change="' + c.id + '"' + actionsDisabled + ' style="background:none;border:none;color:var(--text-ghost);cursor:pointer;padding:0 4px;font-size:16px;line-height:1;" title="Discard this change">&times;</button>' +
      '</div>';
  });
  html += '<div style="display:flex;justify-content:space-between;align-items:center;margin-top:8px;gap:8px;">' +
    '<span style="font-size:11px;color:var(--text-dim);">Use ← → to navigate</span>' +
    '<button class="sync-lightbox-action" data-sync-discard-photo="' + photoId + '"' + actionsDisabled + ' style="background:none;border:1px solid var(--danger);color:var(--danger);border-radius:4px;font-size:11px;cursor:pointer;padding:3px 8px;">Discard all</button>' +
    '</div>';
  panel.innerHTML = html;
}

/** @param {number[]} changeIds @param {number | null} photoId */
async function _syncDiscardFromLightbox(changeIds, photoId) {
  if (!_syncPreviewComplete || !changeIds || changeIds.length === 0) return;
  try {
    await safeFetch('/api/sync/discard', {
      method: 'POST',
      headers: {'Content-Type': 'application/json'},
      body: JSON.stringify({ change_ids: changeIds }),
    });
  } catch(e) { return; }

  await _syncRefreshPreviewAfterDiscard();

  var photoAfter = _syncFindPhotoData(photoId);
  var visibleAfter = _syncVisibleChangesFor(photoAfter);

  renderSyncPreview();

  if (visibleAfter.length > 0) {
    _syncRenderLightboxPanel(photoId);
    return;
  }

  // No more visible changes for this photo — advance the lightbox to the next
  // photo with visible changes, or close if none remain.
  /** @type {{id: number, filename: string}[]} */
  var newList = [];
  if (_syncPreviewData && _syncPreviewData.photos) {
    _syncPreviewData.photos.forEach(function(p) {
      if (_syncVisibleChangesFor(p).length > 0) {
        newList.push({ id: p.photo_id, filename: p.filename });
      }
    });
  }
  var prevList = window._lightboxPhotoList || [];
  var prevIdx = prevList.findIndex(function(p) { return p.id === photoId; });
  if (newList.length === 0) {
    closeLightbox();
    return;
  }
  var nextIdx = prevIdx >= 0 ? Math.min(prevIdx, newList.length - 1) : 0;
  var next = /** @type {{id: number, filename: string}} */ (newList[nextIdx]);
  openLightbox(next.id, next.filename, newList);
}
