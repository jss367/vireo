// The Export Entire Site dialog, its folder picker, and the job.

import { publishFolderBrowser, state } from './state.js';
import { hasPickDirectory, isTauriApp, pickDirectory, safeFetch, showToast } from './shared.js';

export function openSiteExportModal() {
  /** @type {HTMLElement} */ (document.getElementById('siteExportStatus')).textContent = '';
  /** @type {HTMLElement} */ (document.getElementById('siteExportModal')).classList.add('open');
  /** @type {HTMLInputElement} */ (document.getElementById('siteExportDest')).focus();
}

export function closeSiteExportModal() {
  if (!state.siteExportStarting) /** @type {HTMLElement} */ (document.getElementById('siteExportModal')).classList.remove('open');
}

export async function browseSiteExportDest() {
  var input = /** @type {HTMLInputElement} */ (document.getElementById('siteExportDest'));
  if (hasPickDirectory()) {
    var sequence = publishFolderBrowser().sequence;
    try {
      var selected = await pickDirectory('Select Site Export Folder', {
        defaultPath: input.value.trim() || undefined,
      });
      if (selected) {
        input.value = Array.isArray(selected) ? selected[0] || '' : selected;
        return;
      }
      if (isTauriApp()) return;
      if (publishFolderBrowser().sequence !== sequence) {
        publishFolderBrowser().open('siteExport', {skipInitialBrowse: true});
        return;
      }
    } catch (error) {
      console.error('Could not open site export folder picker:', error);
    }
  }
  publishFolderBrowser().open('siteExport');
}

export async function startSiteExport() {
  if (state.siteExportStarting) return;
  var status = /** @type {HTMLElement} */ (document.getElementById('siteExportStatus'));
  var destination = /** @type {HTMLInputElement} */ (document.getElementById('siteExportDest')).value.trim();
  if (!destination) {
    status.textContent = 'Destination folder is required.';
    return;
  }
  var includeLocations = /** @type {HTMLInputElement} */ (document.getElementById('siteExportLocations')).checked;
  state.siteExportStarting = true;
  var controls = document.querySelectorAll(/** @type {'input'} */ ('#siteExportModal input, #siteExportModal button'));
  controls.forEach(function(control) { control.disabled = true; });
  status.textContent = 'Starting site export…';
  try {
    await safeFetch('/api/jobs/export-site', {
      method: 'POST', headers: {'Content-Type': 'application/json'},
      body: JSON.stringify({destination: destination, include_locations: includeLocations}),
    });
    state.siteExportStarting = false;
    closeSiteExportModal();
    if (typeof showToast === 'function') showToast('Site export started. Follow progress in Jobs.', 'info');
  } catch (error) {
    status.textContent = 'Could not start site export: ' + (error instanceof Error ? error.message : String(error));
  } finally {
    state.siteExportStarting = false;
    controls.forEach(function(control) { control.disabled = false; });
  }
}

export function bindSiteExportEscape() {
  document.addEventListener('keydown', function(event) {
    if (event.key === 'Escape' && !/** @type {HTMLElement} */ (document.getElementById('folderBrowser')).classList.contains('open')) {
      closeSiteExportModal();
    }
  });
}
