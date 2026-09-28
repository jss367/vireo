function openReportModal() {
  if (window._reportEscToken) Keymap.popEsc(window._reportEscToken);
  window._reportEscToken = Keymap.pushEsc(function() { closeReportModal(); });

  document.getElementById('reportModal').classList.add('active');
  document.getElementById('reportDescription').value = '';
  document.getElementById('reportStatus').className = 'report-status';
  document.getElementById('reportStatus').textContent = '';
  document.getElementById('reportSendBtn').disabled = false;
  document.getElementById('reportDescription').focus();
}
function closeReportModal() {
  if (window._reportEscToken) { Keymap.popEsc(window._reportEscToken); window._reportEscToken = null; }
  document.getElementById('reportModal').classList.remove('active');
}
function sendReport() {
  var desc = document.getElementById('reportDescription').value.trim();
  if (!desc) {
    document.getElementById('reportDescription').style.borderColor = 'var(--danger)';
    document.getElementById('reportDescription').focus();
    return;
  }
  var btn = document.getElementById('reportSendBtn');
  var status = document.getElementById('reportStatus');
  btn.disabled = true;
  btn.textContent = 'Sending...';
  status.className = 'report-status';
  status.textContent = '';

  fetch('/api/report-issue', {
    method: 'POST',
    headers: {'Content-Type': 'application/json'},
    body: JSON.stringify({description: desc})
  })
  .then(function(r) { return r.json(); })
  .then(function(data) {
    if (data.status === 'sent') {
      status.className = 'report-status success';
      status.textContent = 'Report sent — thanks!';
      setTimeout(closeReportModal, 2000);
    } else if (data.status === 'download') {
      var blob = new Blob([JSON.stringify(data.diagnostics, null, 2)], {type: 'application/json'});
      var url = URL.createObjectURL(blob);
      var a = document.createElement('a');
      a.href = url;
      a.download = 'vireo-issue-report.json';
      a.click();
      URL.revokeObjectURL(url);
      status.className = 'report-status error';
      status.textContent = 'Could not send automatically. Please email the downloaded file.';
    } else if (data.error) {
      status.className = 'report-status error';
      status.textContent = data.error;
    }
    btn.disabled = false;
    btn.textContent = 'Send Report';
  })
  .catch(function(err) {
    status.className = 'report-status error';
    status.textContent = 'Failed to send report: ' + err.message;
    btn.disabled = false;
    btn.textContent = 'Send Report';
  });
}

/* Shared right-click context menu component. */
(function(){
  let _ctxEl = null;
  let _ctxDismiss = null;
  let _ctxEscToken = null;

  window.closeContextMenu = function(){
    if (_ctxEscToken !== null) { Keymap.popEsc(_ctxEscToken); _ctxEscToken = null; }
    if (_ctxEl) { _ctxEl.remove(); _ctxEl = null; }
    document.removeEventListener('mousedown', _outside, true);
    window.removeEventListener('blur', closeContextMenu);
    if (_ctxDismiss) { const f = _ctxDismiss; _ctxDismiss = null; f(); }
  };

  function _outside(e){
    if (_ctxEl && !_ctxEl.contains(e.target)) {
      // Swallow the click that will follow this mousedown so that outside-
      // click dismissal does not also trigger underlying handlers (e.g. the
      // lightbox overlay's onclick=closeLightbox). Only applies when
      // dismissal was triggered by mousedown — Escape / item selection paths
      // don't install the swallow because no click follows.
      const swallow = (ev) => {
        ev.stopPropagation();
        ev.preventDefault();
        window.removeEventListener('click', swallow, true);
      };
      window.addEventListener('click', swallow, true);
      // Safety net: if no click follows (e.g. user released outside the
      // window), remove the swallow listener after a short delay so we
      // don't eat an unrelated later click.
      setTimeout(() => window.removeEventListener('click', swallow, true), 200);
      closeContextMenu();
    }
  }

  function _renderItem(item){
    if (item.separator) {
      const s = document.createElement('div');
      s.className = 'vireo-ctx-sep';
      return s;
    }
    if (item.chips) {
      const row = document.createElement('div');
      row.className = 'vireo-ctx-chips';
      item.chips.forEach(c => {
        const b = document.createElement('span');
        b.className = 'vireo-ctx-chip' + (c.active ? ' is-active' : '') +
          (c.disabled ? ' vireo-ctx-disabled' : '');
        b.textContent = c.label;
        if (c.disabled && c.disabledHint) b.title = c.disabledHint;
        else if (c.title) b.title = c.title;
        if (c.color) {
          b.dataset.color = c.color;
          b.setAttribute('data-color-label-control', '');
          b.dataset.colorLabelBaseTitle = c.colorBaseTitle || c.color;
          b.dataset.colorLabelBaseAria =
            (c.colorBaseTitle || c.color) + ' label';
          b.setAttribute('aria-label', b.dataset.colorLabelBaseAria);
        }
        if (!c.disabled) {
          b.addEventListener('click', ev => {
            ev.stopPropagation();
            closeContextMenu();
            try { c.onClick && c.onClick(); } catch(err){ console.error(err); }
          });
        }
        row.appendChild(b);
      });
      // Fold any already-loaded workspace descriptions into these freshly
      // built chips so their titles and aria-labels stay in sync with the
      // rest of the color-label controls even when the menu opens after
      // descriptions load (no later refreshControls() pass revisits them).
      if (window.VireoColorLabels && typeof window.VireoColorLabels.refreshControls === 'function') {
        window.VireoColorLabels.refreshControls(row);
      }
      return row;
    }
    const d = document.createElement('div');
    d.className = 'vireo-ctx-item' + (item.disabled ? ' vireo-ctx-disabled' : '');
    d.textContent = item.label;
    if (item.disabled && item.disabledHint) d.title = item.disabledHint;
    if (!item.disabled) {
      d.addEventListener('click', ev => {
        ev.stopPropagation();
        closeContextMenu();
        try { item.onClick && item.onClick(); } catch(err){ console.error(err); }
      });
    }
    return d;
  }

  window.openContextMenu = function(event, items, opts){
    closeContextMenu();
    const menu = document.createElement('div');
    menu.className = 'vireo-ctx-menu';
    menu.style.visibility = 'hidden';
    items.forEach(it => menu.appendChild(_renderItem(it)));
    document.body.appendChild(menu);
    // Clamp to viewport.
    const vw = window.innerWidth, vh = window.innerHeight;
    const rect = menu.getBoundingClientRect();
    let x = event.clientX, y = event.clientY;
    if (x + rect.width  > vw) x = Math.max(0, vw - rect.width  - 4);
    if (y + rect.height > vh) y = Math.max(0, vh - rect.height - 4);
    menu.style.left = x + 'px';
    menu.style.top  = y + 'px';
    menu.style.visibility = 'visible';
    _ctxEl = menu;
    _ctxDismiss = (opts && opts.onDismiss) || null;
    document.addEventListener('mousedown', _outside, true);
    // Esc handling: push onto the Keymap stack so a single Esc closes only the
    // context menu (the user dismissed the menu, not the underlying surface).
    _ctxEscToken = Keymap.pushEsc(function() { closeContextMenu(); });
    window.addEventListener('blur', closeContextMenu);
  };

  window.coerceSelectionOnContext = function(selectionSet, clickedId){
    if (clickedId == null) return Array.from(selectionSet);
    if (!selectionSet.has(clickedId)) {
      selectionSet.clear();
      selectionSet.add(clickedId);
    }
    return Array.from(selectionSet);
  };

  // Open Browse mode focused on a photo. In a real browser we open a new tab so
  // the caller (pipeline review, group review, etc.) keeps its state. Inside the
  // Tauri app window there are no tabs and the webview ignores
  // window.open(_, '_blank'), so navigate the single window in place instead —
  // matching how the native View menu switches pages.
  window.openInBrowse = function(photoId){
    var url = '/browse?photo_id=' + photoId;
    if (typeof isTauri === 'function' && isTauri()) {
      var tauriBlockedHint = typeof window.getLightboxBrowseDisabledHint === 'function'
        ? window.getLightboxBrowseDisabledHint(photoId, true)
        : null;
      if (tauriBlockedHint) {
        if (typeof showToast === 'function') showToast(tauriBlockedHint, 'warning');
        return false;
      }
      window.location.href = url;
      return true;
    } else {
      var opened = window.open('about:blank', '_blank');
      if (!opened) {
        var blockedHint = typeof window.getLightboxBrowseDisabledHint === 'function'
          ? window.getLightboxBrowseDisabledHint(photoId, true)
          : null;
        if (blockedHint) {
          if (typeof showToast === 'function') showToast(blockedHint, 'warning');
          return false;
        }
        window.location.href = url;
        return true;
      }
      try { opened.opener = null; } catch (e) {}
      opened.location = url;
      return true;
    }
  };
})();
