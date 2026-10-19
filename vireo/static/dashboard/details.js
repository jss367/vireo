// Starting a metadata sync, and the folder and pending-change detail
// panels.
// ES module.

import { escapeAttr, escapeHtml, safeFetch, showToast } from './shared.js';
import { renderFolderCoverage } from './coverage.js';
import { reloadDashboard } from './loading.js';
import { scopedUrl } from './scope.js';
import { state } from './state.js';

export async function startDashboardSync() {
  if (!window.confirm('Write all pending metadata changes in this workspace to XMP?')) return;
  try {
    var result = /** @type {{job_id?: string} | null} */ (await safeFetch('/api/jobs/sync', {method:'POST'}, {toast:false}));
    showToast('Metadata sync started.', 'success');
    if (result && result.job_id) window.setTimeout(function() { reloadDashboard(); }, 1200);
  } catch (error) {
    showToast('Could not start metadata sync.', 'error');
  }
}

export async function toggleFolderDetail() {
  var panel = /** @type {HTMLElement} */ (document.getElementById('folderDetail'));
  var card = /** @type {HTMLButtonElement} */ (document.getElementById('folderCard'));
  if (state.folderOpen) {
    panel.style.display = 'none';
    state.folderOpen = false;
    card.setAttribute('aria-expanded', 'false');
    return;
  }
  panel.style.display = 'block';
  card.setAttribute('aria-expanded', 'true');
  panel.innerHTML = '<span style="color:var(--text-ghost);font-size:13px;">Loading...</span>';
  state.folderOpen = true;
  try {
    // Coverage endpoint already carries per-folder totals + stage counts, so
    // we don't need a separate /api/folders fetch here.
    if (state._coverageFolders === null) {
      var data = /** @type {{folders?: DashboardCoverageFolder[]}} */ (await safeFetch(scopedUrl('/api/coverage'), {}, { toast: false }));
      state._coverageFolders = data.folders || [];
    }
    var folders = (state._coverageFolders || []).filter(function(f) { return (f.total || 0) > 0; });
    if (folders.length === 0) {
      panel.innerHTML = '<span style="color:var(--text-ghost);font-size:13px;">No folders with photos.</span>';
      return;
    }
    var html = '';
    folders.forEach(function(f, i) {
      var fid = 'fcov_' + i;
      html += '<div class="folder-row">' +
        '<div class="fhead" data-folder-cov="' + fid + '">' +
          '<span class="fchevron">\u25B8</span>' +
          '<span class="fpath" title="' + escapeAttr(f.path) + '">' + escapeHtml(f.path) + '</span>' +
          '<span class="fcount">' + (f.total || 0).toLocaleString() + ' photos</span>' +
          '<button class="attention-action" type="button" style="margin:0 0 0 8px;" ' +
            'data-browse-folder="' + f.folder_id + '">Browse</button>' +
        '</div>' +
        '<div id="' + fid + '" style="display:none;">' + renderFolderCoverage(f) + '</div>' +
      '</div>';
    });
    panel.innerHTML = html;
  } catch(e) {
    panel.innerHTML = '<span style="color:var(--danger);font-size:12px;">Failed to load folders.</span>';
  }
}

/** @param {string} id @param {HTMLElement} head */
export function toggleFolderCov(id, head) {
  var el = document.getElementById(id);
  if (!el) return;
  var open = el.style.display !== 'none';
  el.style.display = open ? 'none' : 'block';
  var chevron = head.querySelector('.fchevron');
  if (chevron) chevron.textContent = open ? '\u25B8' : '\u25BE';
}

export async function togglePendingDetail() {
  var panel = /** @type {HTMLElement} */ (document.getElementById('pendingDetail'));
  var card = /** @type {HTMLButtonElement} */ (document.getElementById('pendingCard'));
  if (state.pendingOpen) {
    panel.style.display = 'none';
    state.pendingOpen = false;
    card.setAttribute('aria-expanded', 'false');
    return;
  }
  panel.style.display = 'block';
  card.setAttribute('aria-expanded', 'true');
  panel.innerHTML = '<span style="color:var(--text-ghost);font-size:13px;">Loading...</span>';
  state.pendingOpen = true;
  try {
    // Keep the Dashboard summary quick even when a workspace has thousands
    // of sidecars on network storage. The full review modal loads every page
    // progressively and exposes exact progress.
    var data = /** @type {DashboardPendingPreview} */ (await safeFetch('/api/sync/preview?limit=50&offset=0', {}, { toast: false }));
    if (data.total_changes === 0) {
      panel.innerHTML = '<span style="color:var(--text-ghost);font-size:13px;">No pending changes.</span>';
      return;
    }
    var html = '<div style="font-size:12px;color:var(--text-dim);margin-bottom:12px;">' + data.total_changes + ' change(s) across ' + data.total_photos + ' photo(s)</div>';
    data.photos.forEach(function(p) {
      html += '<div style="margin-bottom:10px;padding:8px 10px;background:var(--bg-tertiary);border-radius:4px;">' +
        '<div style="font-size:12px;color:var(--text-primary);font-weight:500;margin-bottom:4px;">' + escapeHtml(p.filename) + '</div>' +
        '<div style="font-size:11px;color:var(--text-faint);margin-bottom:6px;">' + escapeHtml(p.folder) + '</div>';
      p.changes.forEach(function(c) {
        var color = c.type === 'keyword_add' ? 'var(--accent)' : c.type === 'keyword_remove' ? 'var(--danger)' : 'var(--text-secondary)';
        var label = c.type === 'keyword_add' ? '+ ' : c.type === 'keyword_remove' ? '- ' : '';
        var presentation = c.presentation || {
          field: c.type.replace(/_/g, ' '),
          before: 'Current XMP value',
          after: c.value || 'Cleared'
        };
        html += '<div style="font-size:12px;color:' + color + ';padding:2px 0;">' +
          label + '<span style="font-weight:600;">' + escapeHtml(presentation.field) + ':</span> ' +
          '<span style="color:var(--text-dim);">' + escapeHtml(presentation.before) + '</span> \u2192 ' +
          escapeHtml(presentation.after) +
          (presentation.after_detail ? '<div style="color:var(--text-ghost);font-size:11px;margin-left:14px;">' + escapeHtml(presentation.after_detail) + '</div>' : '') +
          '</div>';
      });
      html += '</div>';
    });
    if (data.has_more) {
      html += '<div style="font-size:12px;color:var(--text-ghost);padding:8px 2px;">Showing the first ' + data.photos.length + ' photos. Open Review Changes for the full progressive review.</div>';
    }
    panel.innerHTML = html;
  } catch(e) {
    panel.innerHTML = '<span style="color:var(--danger);font-size:12px;">Failed to load pending changes.</span>';
  }
}
