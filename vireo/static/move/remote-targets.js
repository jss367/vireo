// Remote (SSH) move targets: the destination-mode select and the remote target id.

import { state } from './state.js';
import { onQuickFolderModeChange, quickUsesDateFolders } from './quick-move-form.js';
import { updateQuickMoveBtn } from './quick-move-preview.js';
import { safeFetch } from './shared.js';

/* ---------- Remote (SSH) move targets ---------- */

export async function loadRemoteTargets() {
  try {
    var data = await safeFetch('/api/remote-targets');
    state.quickRemoteTargets = (data && data.targets) || [];
    state.quickRsyncAvailable = !!(data && data.rsync_available);
  } catch (e) {
    state.quickRemoteTargets = [];
    state.quickRsyncAvailable = false;
  }
  var sel = /** @type {HTMLSelectElement} */ (document.getElementById('quickDestMode'));
  if (!sel) return;
  // Rebuild remote options, keeping the "Local path" option at index 0.
  while (sel.options.length > 1) sel.remove(1);
  state.quickRemoteTargets.forEach(function(t) {
    var opt = document.createElement('option');
    opt.value = 'remote:' + t.id;
    opt.textContent = t.name + ' — ' + t.user + '@' + t.host + ' (SSH)';
    sel.appendChild(opt);
  });
}

// The remote target id when the destination mode is a remote target, else ''.
export function quickRemoteTargetId() {
  var el = /** @type {HTMLSelectElement} */ (document.getElementById('quickDestMode'));
  var v = el ? el.value : 'local';
  return v.indexOf('remote:') === 0 ? v.slice(7) : '';
}

export function onQuickDestModeChange() {
  var remoteId = quickRemoteTargetId();
  /** @type {HTMLElement} */ (document.getElementById('quickLocalRow')).style.display = remoteId ? 'none' : '';
  /** @type {HTMLElement} */ (document.getElementById('quickRemoteRow')).style.display = remoteId ? '' : 'none';
  var dateOption = /** @type {HTMLOptionElement | null} */ (document.querySelector('#quickFolderMode option[value="date"]'));
  if (dateOption) dateOption.disabled = !!remoteId;
  if (remoteId && quickUsesDateFolders()) {
    /** @type {HTMLSelectElement} */ (document.getElementById('quickFolderMode')).value = 'single';
    onQuickFolderModeChange();
    return;
  }
  updateQuickMoveBtn();
}
