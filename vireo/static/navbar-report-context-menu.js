import { showToast } from './navbar-shared-helpers.js';

/** @type {number | null} */ var _reportEscToken = null;

/** @type {string | null} */
let previewId = null;
let reportRevision = 0;

function reportControls() {
  return {
    description: /** @type {HTMLTextAreaElement} */ (document.getElementById('reportDescription')),
    diagnostics: /** @type {HTMLInputElement} */ (document.getElementById('reportDiagnostics')),
    button: /** @type {HTMLButtonElement} */ (document.getElementById('reportSendBtn')),
    preview: /** @type {HTMLElement} */ (document.getElementById('reportPreview')),
    json: /** @type {HTMLElement} */ (document.getElementById('reportJson')),
    destination: /** @type {HTMLElement} */ (document.getElementById('reportDestination')),
    status: /** @type {HTMLElement} */ (document.getElementById('reportStatus')),
  };
}

export function resetReportPreview() {
  reportRevision++;
  previewId = null;
  const c = reportControls();
  c.preview.hidden = true;
  c.json.textContent = '';
  c.destination.textContent = '';
  c.status.className = 'report-status';
  c.status.textContent = '';
  c.button.disabled = false;
  c.button.textContent = 'Review report';
  c.description.disabled = c.diagnostics.disabled = false;
}

export function openReportModal() {
  if (_reportEscToken) Keymap.popEsc(_reportEscToken);
  _reportEscToken = Keymap.pushEsc(closeReportModal);
  resetReportPreview();
  const c = reportControls();
  c.description.value = '';
  c.diagnostics.checked = false;
  document.getElementById('reportModal')?.classList.add('active');
  c.description.focus();
}

export function closeReportModal() {
  if (_reportEscToken) { Keymap.popEsc(_reportEscToken); _reportEscToken = null; }
  document.getElementById('reportModal')?.classList.remove('active');
  resetReportPreview();
}

export async function sendReport() {
  const c = reportControls();
  const description = c.description.value.trim();
  if (c.button.disabled) return;
  if (!description) {
    c.status.className = 'report-status error';
    c.status.textContent = 'Enter a description to review your report.';
    c.description.focus();
    return;
  }
  const revision = reportRevision;
  const sending = previewId !== null;
  c.button.disabled = true;
  c.description.disabled = c.diagnostics.disabled = sending;
  c.button.textContent = sending ? 'Sending…' : 'Preparing…';
  c.status.textContent = '';
  c.status.className = 'report-status';
  try {
    const response = await fetch('/api/report-issue', {
      method: 'POST',
      headers: {'Content-Type': 'application/json'},
      body: JSON.stringify(sending ? {action: 'send', preview_id: previewId} :
        {action: 'preview', description, include_diagnostics: c.diagnostics.checked}),
    });
    const data = await response.json();
    if (revision !== reportRevision) return;
    if (!response.ok || data.error) throw new Error(data.error || response.statusText);
    if (data.status === 'preview') {
      previewId = data.preview_id;
      c.destination.textContent = data.destination ? 'Send to ' + data.destination : 'Save to this device';
      c.json.textContent = JSON.stringify(data.diagnostics, null, 2);
      c.preview.hidden = false;
      c.button.dataset.delivery = data.destination ? 'send' : 'download';
      c.button.textContent = data.destination ? 'Send report' : 'Download report';
      c.button.disabled = false;
      c.json.focus();
    } else if (data.status === 'sent') {
      c.status.className = 'report-status success';
      c.status.textContent = 'Report sent — thanks!';
      c.button.textContent = 'Sent';
      setTimeout(() => { if (revision === reportRevision) closeReportModal(); }, 2000);
    } else if (data.status === 'download') {
      const automaticSendFailed = c.button.dataset.delivery === 'send';
      const blob = new Blob([JSON.stringify(data.diagnostics, null, 2)], {type: 'application/json'});
      const url = URL.createObjectURL(blob);
      const a = document.createElement('a');
      a.href = url;
      a.download = 'vireo-issue-report.json';
      a.click();
      URL.revokeObjectURL(url);
      resetReportPreview();
      c.status.className = 'report-status success';
      c.status.textContent = automaticSendFailed
        ? 'Could not send automatically. Report downloaded. You can report the issue on '
        : 'Report downloaded. You can report the issue on ';
      const support = document.createElement('a');
      support.href = 'https://github.com/jss367/vireo/issues';
      support.target = '_blank';
      support.rel = 'noopener';
      support.textContent = 'Vireo’s public issue tracker';
      c.status.append(support, '.');
    } else {
      throw new Error('Unexpected response from Vireo');
    }
  } catch (err) {
    if (revision !== reportRevision) return;
    resetReportPreview();
    c.status.className = 'report-status error';
    c.status.textContent = err instanceof Error ? err.message : 'Could not prepare or send the report.';
  }
}

/* Shared right-click context menu component. */
/** @type {HTMLElement | null} */ let _ctxEl = null;
/** @type {(() => void) | null} */ let _ctxDismiss = null;
/** @type {number | null} */ let _ctxEscToken = null;
/** @type {HTMLElement | null} */ let _ctxReturnFocus = null;

export function closeContextMenu(){
  if (_ctxEscToken !== null) { Keymap.popEsc(_ctxEscToken); _ctxEscToken = null; }
  const restoreFocus = _ctxEl?.contains(document.activeElement) ? _ctxReturnFocus : null;
  if (_ctxEl) { _ctxEl.remove(); _ctxEl = null; }
  _ctxReturnFocus = null;
  if (restoreFocus?.isConnected) restoreFocus.focus();
  document.removeEventListener('mousedown', _outside, true);
  window.removeEventListener('blur', closeContextMenu);
  if (_ctxDismiss) { const f = _ctxDismiss; _ctxDismiss = null; f(); }
}

/** @param {Event} e */
function _outside(e){
  if (_ctxEl && !_ctxEl.contains(e.target instanceof Node ? e.target : null)) {
    // Swallow the click that will follow this mousedown so that outside-
    // click dismissal does not also trigger underlying handlers (e.g. the
    // lightbox overlay's onclick=closeLightbox). Only applies when
    // dismissal was triggered by mousedown — Escape / item selection paths
    // don't install the swallow because no click follows.
    const swallow = (/** @type {MouseEvent} */ ev) => {
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

/** @param {HTMLElement} element @param {boolean | undefined} disabled */
function prepareContextMenuItem(element, disabled) {
  element.setAttribute('role', 'menuitem');
  element.setAttribute('aria-disabled', String(!!disabled));
  element.tabIndex = disabled ? -1 : 0;
  element.addEventListener('keydown', e => {
    if (disabled || (e.key !== 'Enter' && e.key !== ' ')) return;
    e.preventDefault();
    e.stopPropagation();
    element.click();
  });
}

/** @param {KeyboardEvent} e */
function navigateContextMenu(e) {
  if (!_ctxEl) return;
  if (e.key === 'Tab' && !e.ctrlKey && !e.metaKey && !e.altKey) {
    closeContextMenu();
    return;
  }
  if (!['ArrowDown', 'ArrowUp', 'Home', 'End'].includes(e.key)) return;
  const items = Array.from(/** @type {NodeListOf<HTMLElement>} */ (
    _ctxEl.querySelectorAll('[role="menuitem"]:not([aria-disabled="true"])')));
  if (!items.length) return;
  const current = items.findIndex(item => item === document.activeElement);
  let next = e.key === 'ArrowUp' ? (current < 0 ? items.length - 1 : current - 1)
    : current + 1;
  if (e.key === 'Home') next = 0;
  if (e.key === 'End') next = items.length - 1;
  e.preventDefault();
  e.stopPropagation();
  items[(next + items.length) % items.length]?.focus();
}

/** @param {NavMenuItem} item */
function _renderItem(item){
  if (item.separator) {
    const s = document.createElement('div');
    s.className = 'vireo-ctx-sep';
    s.setAttribute('role', 'separator');
    return s;
  }
  if (item.chips) {
    const row = document.createElement('div');
    row.className = 'vireo-ctx-chips';
    item.chips.forEach(c => {
      const b = document.createElement('span');
      b.className = 'vireo-ctx-chip' + (c.active ? ' is-active' : '') +
        (c.disabled ? ' vireo-ctx-disabled' : '');
      b.textContent = c.label ?? '';
      prepareContextMenuItem(b, c.disabled);
      if (c.title) b.setAttribute('aria-label', c.title);
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
  d.textContent = item.label ?? '';
  prepareContextMenuItem(d, item.disabled);
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

/** @type {NavbarApi['openContextMenu']} */ export function openContextMenu(event, items, opts){
  closeContextMenu();
  const menu = document.createElement('div');
  menu.className = 'vireo-ctx-menu';
  menu.setAttribute('role', 'menu');
  menu.setAttribute('aria-label', 'Actions');
  menu.addEventListener('keydown', navigateContextMenu);
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
  _ctxReturnFocus = document.activeElement instanceof HTMLElement ? document.activeElement : null;
  _ctxDismiss = (opts && opts.onDismiss) || null;
  document.addEventListener('mousedown', _outside, true);
  // Esc handling: push onto the Keymap stack so a single Esc closes only the
  // context menu (the user dismissed the menu, not the underlying surface).
  _ctxEscToken = Keymap.pushEsc(function() { closeContextMenu(); });
  window.addEventListener('blur', closeContextMenu);
}

/** @param {Set<number>} selectionSet @param {number | null} clickedId @returns {number[]} */
export function coerceSelectionOnContext(selectionSet, clickedId){
  if (clickedId == null) return Array.from(selectionSet);
  if (!selectionSet.has(clickedId)) {
    selectionSet.clear();
    selectionSet.add(clickedId);
  }
  return Array.from(selectionSet);
}

// Open Browse mode focused on a photo. In a real browser we open a new tab so
// the caller (pipeline review, group review, etc.) keeps its state. Inside the
// Tauri app window there are no tabs and the webview ignores
// window.open(_, '_blank'), so navigate the single window in place instead —
// matching how the native View menu switches pages.
/** @param {number} photoId */
export function openInBrowse(photoId){
  var url = '/browse?photo_id=' + photoId;
  var browseDisabledHint = Vireo.pageHook('getLightboxBrowseDisabledHint');
  if (typeof isTauri === 'function' && isTauri()) {
    var tauriBlockedHint = browseDisabledHint ? browseDisabledHint(photoId, true) : null;
    if (tauriBlockedHint) {
      if (typeof showToast === 'function') showToast(tauriBlockedHint, 'warning');
      return false;
    }
    window.location.href = url;
    return true;
  } else {
    var opened = window.open('about:blank', '_blank');
    if (!opened) {
      var blockedHint = browseDisabledHint ? browseDisabledHint(photoId, true) : null;
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
}
