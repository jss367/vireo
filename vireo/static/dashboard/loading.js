// Loading the scope options and reloading every section for the current
// scope.
// ES module.

import { safeFetch, showToast } from './shared.js';
import { loadCoverage } from './coverage.js';
import { loadClassificationInventory } from './inventory.js';
import { loadStats } from './overview.js';
import { dashboardDateRangeIsValid, dashboardScopeIsActive, readDashboardScopeControls, updateScopeSummary } from './scope.js';
import { state } from './state.js';

export async function loadDashboardOptions() {
  var data = /** @type {DashboardOptions | null} */ (await safeFetch('/api/dashboard/options', {}, {toast:false}));
  state.dashboardOptions = data || {folders: [], collections: []};
  var folderSelect = /** @type {HTMLSelectElement} */ (document.getElementById('scopeFolder'));
  var collectionSelect = /** @type {HTMLSelectElement} */ (document.getElementById('scopeCollection'));
  (state.dashboardOptions.folders || []).forEach(function(folder) {
    var option = document.createElement('option');
    option.value = String(folder.id);
    option.textContent = folder.path;
    folderSelect.appendChild(option);
  });
  (state.dashboardOptions.collections || []).forEach(function(collection) {
    var option = document.createElement('option');
    option.value = String(collection.id);
    // Dashboard scope (/api/stats, /api/coverage, and the Browse
    // drilldown link) evaluates rules only via _build_collection_query,
    // so a visual collection selected here would silently widen to every
    // metadata match. The has_visual flag surfaces from
    // /api/dashboard/options; disable those options and 400 server-side
    // as belt-and-suspenders.
    var suffix = '';
    if (collection.degraded) suffix = ' (unavailable — rule error)';
    else if (collection.has_visual) suffix = ' (visual — open from Browse)';
    option.textContent = collection.name + suffix;
    if (collection.degraded || collection.has_visual) option.disabled = true;
    collectionSelect.appendChild(option);
  });

  var query = new URLSearchParams(window.location.search);
  var folderId = query.get('folder_id') || '';
  var collectionId = query.get('collection_id') || '';
  if (folderId && folderSelect.querySelector('option[value="' + CSS.escape(folderId) + '"]')) {
    folderSelect.value = folderId;
  } else if (collectionId) {
    // A bookmarked /dashboard?collection_id=... whose rules have since
    // broken still matches an <option>, but that option is disabled
    // (marked degraded by /api/dashboard/options). Programmatically
    // selecting it would send the id to /api/stats and /api/coverage,
    // both of which 400 via _build_collection_query and leave the
    // Dashboard wedged until the scope is reset. Skip disabled options.
    var opt = /** @type {HTMLOptionElement | null} */ (collectionSelect.querySelector(
      'option[value="' + CSS.escape(collectionId) + '"]'));
    if (opt && !opt.disabled) collectionSelect.value = collectionId;
  }
  /** @type {HTMLInputElement} */ (document.getElementById('scopeDateFrom')).value = query.get('date_from') || '';
  /** @type {HTMLInputElement} */ (document.getElementById('scopeDateTo')).value = query.get('date_to') || '';
  readDashboardScopeControls();
  if (!dashboardDateRangeIsValid()) {
    showToast('The start date must be before the end date.', 'error');
    /** @type {HTMLInputElement} */ (document.getElementById('scopeDateFrom')).value = '';
    /** @type {HTMLInputElement} */ (document.getElementById('scopeDateTo')).value = '';
    readDashboardScopeControls();
    query.delete('date_from');
    query.delete('date_to');
    var cleanQuery = query.toString();
    window.history.replaceState({}, '', '/dashboard' + (cleanQuery ? '?' + cleanQuery : ''));
  }
  updateScopeSummary();
}

export async function reloadDashboard() {
  var panel = /** @type {HTMLElement} */ (document.getElementById('scopePanel'));
  var refresh = /** @type {HTMLButtonElement} */ (document.getElementById('dashboardRefresh'));
  panel.classList.add('dashboard-busy');
  refresh.disabled = true;
  state.folderOpen = false;
  state.pendingOpen = false;
  /** @type {HTMLElement} */ (document.getElementById('folderDetail')).style.display = 'none';
  /** @type {HTMLElement} */ (document.getElementById('pendingDetail')).style.display = 'none';
  /** @type {HTMLButtonElement} */ (document.getElementById('folderCard')).setAttribute('aria-expanded', 'false');
  /** @type {HTMLButtonElement} */ (document.getElementById('pendingCard')).setAttribute('aria-expanded', 'false');
  state._coverageFolders = null;
  try {
    await Promise.all([loadStats(), loadCoverage()]);
    var inventory = /** @type {HTMLElement} */ (document.getElementById('classInvSection'));
    if (dashboardScopeIsActive()) {
      inventory.style.display = 'none';
    } else {
      inventory.style.display = '';
      await loadClassificationInventory();
    }
  } finally {
    panel.classList.remove('dashboard-busy');
    refresh.disabled = false;
  }
}

export async function bootstrapDashboard() {
  try { await loadDashboardOptions(); }
  catch (error) { showToast('Could not load Dashboard scope options.', 'error'); }
  await reloadDashboard();
}
