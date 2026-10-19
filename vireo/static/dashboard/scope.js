// The Dashboard scope (folder, collection, capture dates): the query it
// adds to every request and link, the scope controls and summary, and
// opening Browse or Process with the same scope.
// ES module.

import { showToast } from './shared.js';
import { reloadDashboard } from './loading.js';
import { state } from './state.js';

/** @param {DashboardBrowseParams=} extra */
export function scopedParams(extra) {
  var params = new URLSearchParams();
  if (state.dashboardScope.folder_id) params.set('folder_id', state.dashboardScope.folder_id);
  if (state.dashboardScope.collection_id) params.set('collection_id', state.dashboardScope.collection_id);
  if (state.dashboardScope.date_from) params.set('date_from', state.dashboardScope.date_from);
  if (state.dashboardScope.date_to) params.set('date_to', state.dashboardScope.date_to);
  Object.keys(extra || {}).forEach(function(key) {
    var value = /** @type {DashboardBrowseParams} */ (extra)[key];
    if (value !== null && value !== undefined && value !== '') params.set(key, String(value));
  });
  return params;
}

/** @param {string} path @param {DashboardBrowseParams=} extra */
export function scopedUrl(path, extra) {
  var qs = scopedParams(extra).toString();
  return path + (qs ? '?' + qs : '');
}

/** @param {DashboardBrowseParams=} extra */
export function openBrowse(extra) {
  var browseParams = Object.assign({dashboard_scope: '1'}, extra || {});
  window.location.href = scopedUrl('/browse', browseParams);
}

export function selectedFolderPath() {
  var id = parseInt(state.dashboardScope.folder_id || '', 10);
  var folder = state.dashboardOptions.folders.find(function(f) { return f.id === id; });
  return folder ? folder.path : '';
}

export function processScopeIssues() {
  // Pipeline recognises top-level workspace roots and collections; it maps
  // child-folder paths to the top-level root that contains them and has no
  // capture-date filter. Passing a narrower Dashboard scope would silently
  // run Process across a wider set than the attention card counted.
  /** @type {string[]} */
  var issues = [];
  if (state.dashboardScope.folder_id) {
    var id = parseInt(state.dashboardScope.folder_id, 10);
    /** @type {Record<number, DashboardFolderOption>} */
    var byId = {};
    (state.dashboardOptions.folders || []).forEach(function(f) { byId[f.id] = f; });
    var folder = byId[id];
    if (folder && folder.parent_id && byId[folder.parent_id]) {
      issues.push('the child folder scope (Process would expand it to the '
        + 'containing top-level folder)');
    }
  }
  if (state.dashboardScope.date_from || state.dashboardScope.date_to) {
    issues.push('the capture-date range (Process has no capture-date filter)');
  }
  return issues;
}

export function processScopeBlockedReason() {
  // The attention cards' Process shortcut is only meaningful when the
  // scope the card counted is the same scope Process will run over. When
  // it isn't (child folder, capture-date range), silently over-scoping
  // Process runs previews/classification over sibling folders or dates
  // the card never counted, which is the opposite of what the user
  // clicked on. Return a human-readable reason so the button can be
  // disabled with an explanation instead of relying on a confirm dialog.
  var issues = processScopeIssues();
  if (!issues.length) return '';
  return 'Process cannot preserve ' + issues.join(' or ') +
    '. Adjust the Dashboard scope, or open Process from the menu to run it deliberately.';
}

/** @param {string} stage */
export function openProcess(stage) {
  if (processScopeBlockedReason()) return;
  var params = new URLSearchParams();
  params.set('dashboard_stage', stage);
  var folderPath = selectedFolderPath();
  if (folderPath) params.append('folder', folderPath);
  if (state.dashboardScope.collection_id) params.set('collection_id', state.dashboardScope.collection_id);
  window.location.href = '/pipeline?' + params.toString();
}

export function dashboardScopeIsActive() {
  return !!(state.dashboardScope.folder_id || state.dashboardScope.collection_id ||
    state.dashboardScope.date_from || state.dashboardScope.date_to);
}

export function readDashboardScopeControls() {
  state.dashboardScope.folder_id = /** @type {HTMLSelectElement} */ (document.getElementById('scopeFolder')).value || null;
  state.dashboardScope.collection_id = /** @type {HTMLSelectElement} */ (document.getElementById('scopeCollection')).value || null;
  state.dashboardScope.date_from = /** @type {HTMLInputElement} */ (document.getElementById('scopeDateFrom')).value || '';
  state.dashboardScope.date_to = /** @type {HTMLInputElement} */ (document.getElementById('scopeDateTo')).value || '';
}

export function onFolderScopeChange() {
  if (/** @type {HTMLSelectElement} */ (document.getElementById('scopeFolder')).value) {
    /** @type {HTMLSelectElement} */ (document.getElementById('scopeCollection')).value = '';
  }
}

export function onCollectionScopeChange() {
  if (/** @type {HTMLSelectElement} */ (document.getElementById('scopeCollection')).value) {
    /** @type {HTMLSelectElement} */ (document.getElementById('scopeFolder')).value = '';
  }
}

export function applyDashboardScope() {
  readDashboardScopeControls();
  if (!dashboardDateRangeIsValid()) {
    showToast('The start date must be before the end date.', 'error');
    return;
  }
  var url = scopedUrl('/dashboard');
  window.history.replaceState({}, '', url);
  updateScopeSummary();
  reloadDashboard();
}

export function dashboardDateRangeIsValid() {
  return !(state.dashboardScope.date_from && state.dashboardScope.date_to &&
    state.dashboardScope.date_from > state.dashboardScope.date_to);
}

export function resetDashboardScope() {
  /** @type {HTMLSelectElement} */ (document.getElementById('scopeFolder')).value = '';
  /** @type {HTMLSelectElement} */ (document.getElementById('scopeCollection')).value = '';
  /** @type {HTMLInputElement} */ (document.getElementById('scopeDateFrom')).value = '';
  /** @type {HTMLInputElement} */ (document.getElementById('scopeDateTo')).value = '';
  applyDashboardScope();
}

export function updateScopeSummary() {
  var parts = [];
  if (state.dashboardScope.folder_id) {
    var folderPath = selectedFolderPath();
    parts.push(folderPath ? 'Folder: ' + folderPath : 'Selected folder');
  }
  if (state.dashboardScope.collection_id) {
    var id = parseInt(state.dashboardScope.collection_id, 10);
    var collection = state.dashboardOptions.collections.find(function(c) { return c.id === id; });
    parts.push(collection ? 'Collection: ' + collection.name : 'Selected collection');
  }
  if (state.dashboardScope.date_from && state.dashboardScope.date_to) {
    parts.push(state.dashboardScope.date_from + ' to ' + state.dashboardScope.date_to);
  } else if (state.dashboardScope.date_from) {
    parts.push('From ' + state.dashboardScope.date_from + ' onward');
  } else if (state.dashboardScope.date_to) {
    parts.push('Through ' + state.dashboardScope.date_to);
  }
  /** @type {HTMLElement} */ (document.getElementById('scopeSummary')).textContent = parts.length
    ? parts.join(' · ') : 'All photos in this workspace';
}
