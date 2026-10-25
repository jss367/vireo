// The Masks card and its Manage modal: per-variant usage and deletes.

import { state } from './state.js';
import { storageEscapeHtml, storageFormatBytes } from './format.js';
import { loadStoragePanel } from './panel.js';
import { safeFetch } from './shared.js';

/** @param {StorageMasks | undefined} storageData The masks summary /api/storage already returned, if any. */
export async function loadMasksCard(storageData) {
  var card = /** @type {HTMLElement} */ (document.getElementById('masksCard'));
  try {
    var data = storageData || /** @type {StorageMasks} */ (await safeFetch('/api/storage/masks', {}, { toast: false }));
    state.masksData = data;
    var variants = data.variants || [];
    var totalBytes = data.total_bytes || 0;
    var stale = data.stale_count || 0;

    var inner = '<div style="background:var(--bg-tertiary);border-radius:6px;padding:14px 16px;">';
    inner += '<div style="display:flex;align-items:center;justify-content:space-between;gap:12px;margin-bottom:10px;">';
    inner += '<div style="font-size:13px;font-weight:600;color:var(--text-primary);">Masks ' +
      '<span style="font-size:11px;font-weight:400;color:var(--text-dim);margin-left:6px;">' +
      storageFormatBytes(totalBytes) + '</span> ' +
      '<span class="stat-kind keep" style="margin-left:6px;">Review before clearing</span></div>';
    inner += '<div style="display:flex;gap:6px;flex-wrap:wrap;justify-content:flex-end;">';
    inner += '<button class="btn-manage" data-masks-action="manage"' +
      (variants.length === 0 ? ' disabled style="opacity:0.5;cursor:default;"' : '') +
      '>Manage</button>';
    inner += '<button class="btn-secondary" data-masks-action="delete-inactive"' +
      (variants.length === 0 ? ' disabled style="opacity:0.5;cursor:default;"' : '') +
      '>Delete inactive</button>';
    if (stale > 0) {
      inner += '<button class="btn-secondary" data-masks-action="delete-stale" style="color:var(--warning);border-color:var(--warning);">Delete stale (' + stale + ')</button>';
    }
    inner += '</div></div>';

    if (variants.length === 0) {
      inner += '<div style="font-size:12px;color:var(--text-ghost);">No SAM masks yet.</div>';
    } else {
      inner += '<div style="font-size:11px;color:var(--text-dim);margin-bottom:8px;">Generated from detections; deleting them may require another mask-generation run.</div>';
      inner += '<div style="display:flex;flex-direction:column;gap:4px;">';
      variants.forEach(function(v) {
        var isActive = (v.active_count || 0) > 0;
        inner += '<div style="display:flex;align-items:center;gap:10px;font-size:12px;font-variant-numeric:tabular-nums;">';
        inner += '<span style="min-width:120px;color:var(--text-secondary);">' + storageEscapeHtml(v.variant) + '</span>';
        inner += '<span style="min-width:80px;color:var(--text-dim);">' + (v.count || 0).toLocaleString() + ' photo' + (v.count === 1 ? '' : 's') + '</span>';
        inner += '<span style="min-width:80px;color:var(--text-faint);">' + storageFormatBytes(v.bytes || 0) + '</span>';
        if (isActive) {
          inner += '<span style="font-size:10px;color:var(--accent);background:color-mix(in srgb, var(--accent) 18%, transparent);border-radius:3px;padding:1px 6px;">active - ' +
            (v.active_count || 0).toLocaleString() + '</span>';
        }
        inner += '</div>';
      });
      inner += '</div>';
    }
    inner += '</div>';
    card.innerHTML = inner;
  } catch(e) {
    card.innerHTML = '<div style="background:var(--bg-tertiary);border-radius:6px;padding:14px 16px;">' +
      '<span style="color:var(--danger);font-size:12px;">Failed to load mask info</span></div>';
  }
}

export function openMasksModal() {
  if (!state.masksData) return;
  /** @type {HTMLElement} */ (document.getElementById('masksModalError')).style.display = 'none';
  renderMasksModalRows();
  /** @type {HTMLElement} */ (document.getElementById('masksModal')).classList.add('open');
}

export function closeMasksModal() {
  /** @type {HTMLElement} */ (document.getElementById('masksModal')).classList.remove('open');
}

export function renderMasksModalRows() {
  var rows = /** @type {HTMLElement} */ (document.getElementById('masksModalRows'));
  rows.textContent = '';
  if (!state.masksData || (state.masksData.variants || []).length === 0) {
    var empty = document.createElement('li');
    empty.style.color = 'var(--text-ghost)';
    empty.style.fontSize = '13px';
    empty.style.padding = '12px 0';
    empty.textContent = 'No mask variants present.';
    rows.appendChild(empty);
    return;
  }
  (state.masksData.variants || []).forEach(function(v) {
    var isActive = (v.active_count || 0) > 0;
    var li = document.createElement('li');
    li.className = 'file-item';
    var label = document.createElement('label');
    label.style.flex = '1';
    var strong = document.createElement('strong');
    strong.style.color = 'var(--text-primary)';
    strong.textContent = v.variant;
    label.appendChild(strong);
    var meta = document.createElement('span');
    meta.style.color = 'var(--text-dim)';
    meta.textContent = ' - ' + (v.count || 0).toLocaleString() +
      ' photos - ' + storageFormatBytes(v.bytes || 0);
    label.appendChild(meta);
    if (isActive) {
      var activeTag = document.createElement('span');
      activeTag.style.fontSize = '10px';
      activeTag.style.color = 'var(--accent)';
      activeTag.style.marginLeft = '6px';
      activeTag.textContent = '[active - ' + (v.active_count || 0).toLocaleString() + ']';
      label.appendChild(activeTag);
    }
    li.appendChild(label);
    var btn = document.createElement('button');
    btn.className = 'btn-danger';
    btn.textContent = 'Delete';
    if (isActive) {
      btn.disabled = true;
      btn.title = 'set another variant active first';
    } else {
      btn.addEventListener('click', function() { deleteMaskVariant(v.variant); });
    }
    li.appendChild(btn);
    rows.appendChild(li);
  });
}

/** @param {string} msg */
export function _showMasksModalError(msg) {
  var el = /** @type {HTMLElement} */ (document.getElementById('masksModalError'));
  el.textContent = msg;
  el.style.display = '';
}

/** @param {string} variant */
export async function deleteMaskVariant(variant) {
  if (!confirm('Delete all "' + variant + '" masks? Files and DB rows will be removed.')) return;
  try {
    var resp = await fetch('/api/storage/masks/delete-variant', {
      method: 'POST',
      headers: {'Content-Type': 'application/json'},
      body: JSON.stringify({variant: variant}),
    });
    var body = await resp.json();
    if (!resp.ok) {
      _showMasksModalError(body.message || body.error || ('HTTP ' + resp.status));
      return;
    }
    /** @type {HTMLElement} */ (document.getElementById('masksModalError')).style.display = 'none';
  } catch(e) {
    _showMasksModalError(String(e));
    return;
  }
  await loadStoragePanel();
  if (state.masksData && (state.masksData.variants || []).length > 0) renderMasksModalRows();
  else closeMasksModal();
}

export async function deleteInactiveMasks() {
  if (!confirm('Delete all non-active mask variants across photos? The active variant per photo is kept.')) return;
  try {
    await safeFetch('/api/storage/masks/delete-inactive', {
      method: 'POST',
      headers: {'Content-Type': 'application/json'},
    });
  } catch(e) {}
  await loadStoragePanel();
}

export async function deleteStaleMasks() {
  if (!confirm('Delete masks whose detection prompt no longer matches the photo primary detection?')) return;
  try {
    await safeFetch('/api/storage/masks/delete-stale', {
      method: 'POST',
      headers: {'Content-Type': 'application/json'},
    });
  } catch(e) {}
  await loadStoragePanel();
}
