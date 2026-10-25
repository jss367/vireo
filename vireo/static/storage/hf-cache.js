/* Hugging Face cache card and modal on the Storage page.
 *
 * Older Vireo downloads landed here before being copied into the models
 * folder, so each model took its size twice; downloads now skip the cache.
 * Settings only ever shows the models folder. This modal lists the cache per
 * model, identifies existing model folders, and removes entries, then
 * reloads the cards through loadStoragePanel.
 */

import { state } from './state.js';
import { storageFormatBytes } from './format.js';
import { loadStoragePanel } from './panel.js';
import { safeFetch, showToast } from './shared.js';

/** @param {{size?:number;unused_size?:number;retained_size?:number;installed_size?:number} | null | undefined} hf @returns {string | null} */
export function hfCacheCardNote(hf) {
  if (!hf || !hf.size) return null;
  if (hf.unused_size) {
    return storageFormatBytes(hf.unused_size) + ' backs no installed model';
  }
  if (hf.retained_size) return storageFormatBytes(hf.retained_size) + ' retained cache payloads';
  return hf.installed_size
    ? storageFormatBytes(hf.installed_size) + ' cached for existing model folders'
    : 'No removable payloads';
}

export async function openHfCacheModal() {
  var modal = /** @type {HTMLElement} */ (document.getElementById('hfCacheModal'));
  /** @type {HTMLElement} */ (document.getElementById('hfCacheError')).style.display = 'none';
  /** @type {HTMLElement} */ (document.getElementById('hfCacheLoading')).style.display = 'block';
  /** @type {HTMLElement} */ (document.getElementById('hfCacheRows')).textContent = '';
  /** @type {HTMLButtonElement} */ (document.getElementById('hfCacheRemoveUnused')).disabled = true;
  /** @type {HTMLButtonElement} */ (document.getElementById('hfCacheRemoveInstalled')).disabled = true;
  modal.classList.add('open');
  await loadHfCacheRows();
}

export function closeHfCacheModal() {
  /** @type {HTMLElement} */ (document.getElementById('hfCacheModal')).classList.remove('open');
}

export async function loadHfCacheRows() {
  try {
    state.hfCacheInventory = await safeFetch('/api/storage/hf-cache', {}, { toast: false });
  } catch (e) {
    /** @type {HTMLElement} */ (document.getElementById('hfCacheLoading')).style.display = 'none';
    _showHfCacheError('Failed to load the Hugging Face cache.');
    return;
  }
  /** @type {HTMLElement} */ (document.getElementById('hfCacheLoading')).style.display = 'none';
  renderHfCacheRows();
}

/** @param {string} msg */
export function _showHfCacheError(msg) {
  var el = /** @type {HTMLElement} */ (document.getElementById('hfCacheError'));
  el.textContent = msg;
  el.style.display = '';
}

/** @param {HfCacheEntry} entry @returns {HTMLLIElement} */
export function _hfCacheRow(entry) {
  var li = document.createElement('li');
  li.className = 'file-item';
  li.style.padding = '8px 0';
  var label = document.createElement('div');
  label.style.flex = '1';
  label.style.minWidth = '0';
  var strong = document.createElement('div');
  strong.style.color = 'var(--text-primary)';
  strong.style.fontWeight = '600';
  strong.textContent = entry.name;
  label.appendChild(strong);
  var meta = document.createElement('div');
  meta.style.cssText = 'display:flex;align-items:center;gap:8px;flex-wrap:wrap;margin-top:3px;color:var(--text-dim);';
  var files = document.createElement('span');
  files.textContent = (entry.file_count === 1 ? '1 file' : entry.file_count + ' files') +
    ' · ' + storageFormatBytes(entry.size);
  if (entry.retained_size) files.textContent += ' · ' + storageFormatBytes(entry.retained_size) + ' retained';
  meta.appendChild(files);
  var tag = document.createElement('span');
  tag.className = 'stat-kind ' + (entry.removable === false ? 'keep' : 'safe');
  tag.textContent = entry.installed ? 'Model folder exists' : 'No installed model uses this';
  if (entry.installed && entry.install_path) {
    tag.title = 'Model folder: ' + entry.install_path + '. It may be incomplete; removing this cache can require downloading missing files again.';
  }
  meta.appendChild(tag);
  label.appendChild(meta);
  li.appendChild(label);
  var btn = document.createElement('button');
  btn.className = 'btn-danger';
  btn.textContent = 'Remove';
  if (entry.retained_reason) {
    btn.disabled = true;
    btn.title = entry.retained_reason;
  } else if (entry.removable === false) {
    btn.disabled = true;
    btn.title = 'Snapshot copies are retained because their download lock cannot be verified.';
  } else {
    btn.addEventListener('click', function() { removeHfCacheEntry(entry); });
  }
  li.appendChild(btn);
  return li;
}

export function renderHfCacheRows() {
  var rows = /** @type {HTMLElement} */ (document.getElementById('hfCacheRows'));
  rows.textContent = '';
  /** @type {HfCacheInventory} */ var inv = state.hfCacheInventory || {entries: [], unused_size: 0};
  if (!inv.entries.length) {
    var empty = document.createElement('li');
    empty.style.color = 'var(--text-ghost)';
    empty.style.fontSize = '13px';
    empty.style.padding = '12px 0';
    empty.textContent = 'No Vireo models in the Hugging Face cache.';
    rows.appendChild(empty);
  }
  inv.entries.forEach(function(entry) { rows.appendChild(_hfCacheRow(entry)); });
  var btn = /** @type {HTMLButtonElement} */ (document.getElementById('hfCacheRemoveUnused'));
  btn.disabled = !inv.unused_size;
  btn.textContent = inv.unused_size
    ? 'Remove unused entries · ' + storageFormatBytes(inv.unused_size)
    : 'No unused entries';
  var installed = inv.entries.filter(function(entry) { return entry.installed && entry.removable !== false; });
  var installedBtn = /** @type {HTMLButtonElement} */ (document.getElementById('hfCacheRemoveInstalled'));
  installedBtn.disabled = !installed.length;
  installedBtn.textContent = 'Remove caches for existing model folders' + (installed.length ? ' · ' + installed.length : '');
}

/** @param {{keys?:string[];unused?:boolean;installed?:boolean}} body @param {string} failure */
export async function _postHfCacheRemove(body, failure) {
  /** @type {HTMLElement} */ (document.getElementById('hfCacheError')).style.display = 'none';
  try {
    /** @type {HfCacheInventory} */ var result = await safeFetch('/api/storage/hf-cache/remove', {
      method: 'POST',
      headers: {'Content-Type': 'application/json'},
      body: JSON.stringify(body),
    }, { toast: false });
    state.hfCacheInventory = result;
    var first = result.failed && result.failed[0];
    if (first) {
      // Some paths could not be deleted; they stay in the refreshed list.
      var failed = result.failed || [];
      _showHfCacheError(failure + ': ' + failed.length +
        (failed.length === 1 ? ' file' : ' files') + ' could not be deleted (' +
        first.path + ': ' + first.error + '). Freed ' + storageFormatBytes(result.freed || 0) + '.');
    } else {
      showToast('Freed ' + storageFormatBytes(result.freed || 0) + ' from the Hugging Face cache', 'success');
    }
  } catch (e) {
    var message = e instanceof Error ? e.message : '';
    _showHfCacheError(failure + (message ? ': ' + message : '.'));
    await loadHfCacheRows();
    return;
  }
  renderHfCacheRows();
  loadStoragePanel();
}

/** @param {HfCacheEntry} entry */
export async function removeHfCacheEntry(entry) {
  if (!confirm('Remove "' + entry.name + '" (' + storageFormatBytes(entry.size) +
      ') from the Hugging Face cache? Installing that model again will download it from Hugging Face.')) return;
  await _postHfCacheRemove({keys: [entry.key]}, 'Failed to remove the cache entry');
}

export async function removeUnusedHfCache() {
  var inv = state.hfCacheInventory;
  if (!inv || !inv.unused_size) return;
  var count = inv.entries.filter(function(e) { return !e.installed && e.removable !== false; }).length;
  if (!confirm('Remove ' + count + (count === 1 ? ' entry' : ' entries') + ' (' +
      storageFormatBytes(inv.unused_size) + ') that no installed model uses? ' +
      'Installing those models again will download them from Hugging Face.')) return;
  await _postHfCacheRemove({unused: true}, 'Failed to remove unused cache entries');
}

export async function removeInstalledHfCache() {
  var inv = state.hfCacheInventory;
  if (!inv) return;
  var count = inv.entries.filter(function(entry) { return entry.installed && entry.removable !== false; }).length;
  if (!count) return;
  if (!confirm('Remove ' + count + ' cache ' + (count === 1 ? 'entry' : 'entries') +
      ' associated with existing model folders? Installed files stay in place. ' +
      'A folder may be incomplete or a different revision; repair may need another download. ' +
      'Other applications sharing these caches may also need to download them again.')) return;
  await _postHfCacheRemove({installed: true}, 'Failed to remove caches for existing model folders');
}
