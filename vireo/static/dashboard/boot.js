// Page startup: the module the template loads. Binds the controls that used
// to carry inline handlers, the delegated listeners on rendered markup (bar
// drill-downs, inventory model rows, folder coverage rows, attention cards),
// and loads the Dashboard on DOMContentLoaded.
// ES module.

import { startDashboardSync, toggleFolderCov, toggleFolderDetail, togglePendingDetail } from './details.js';
import { activateDashboardDrill } from './drill.js';
import { loadClassificationInventory, toggleInvModel } from './inventory.js';
import { bootstrapDashboard, reloadDashboard } from './loading.js';
import { applyDashboardScope, onCollectionScopeChange, onFolderScopeChange, openBrowse, openProcess, resetDashboardScope } from './scope.js';

/** @param {string} id */
function dashboardEl(id) {
  return /** @type {HTMLElement} */ (document.getElementById(id));
}

/** @param {string} id @param {() => unknown} action */
function onDashboardClick(id, action) {
  dashboardEl(id).addEventListener('click', function() { action(); });
}

onDashboardClick('dashboardRefresh', reloadDashboard);
dashboardEl('scopeFolder').addEventListener('change', onFolderScopeChange);
dashboardEl('scopeCollection').addEventListener('change', onCollectionScopeChange);
onDashboardClick('scopeApply', applyDashboardScope);
onDashboardClick('scopeReset', resetDashboardScope);
onDashboardClick('photoCard', function() { openBrowse(); });
onDashboardClick('folderCard', toggleFolderDetail);
onDashboardClick('pendingCard', togglePendingDetail);
onDashboardClick('classInvRefresh', loadClassificationInventory);

/** @param {Event} event @param {string} selector */
function closestTarget(event, selector) {
  var target = event.target instanceof Element ? event.target.closest(selector) : null;
  return target instanceof HTMLElement ? target : null;
}

// Bar rows anywhere on the page open the matching photos in Browse.
document.addEventListener('click', activateDashboardDrill);
document.addEventListener('keydown', function(event) {
  if (event.key !== 'Enter' && event.key !== ' ') return;
  if (!closestTarget(event, '[data-dashboard-drill]')) return;
  event.preventDefault();
  activateDashboardDrill(event);
});

// Inventory model header rows expand and collapse their label rows.
dashboardEl('classInvBody').addEventListener('click', function(event) {
  var row = closestTarget(event, '[data-inv-model]');
  if (row) toggleInvModel(Number(row.dataset.invModel));
});

// Folder coverage: a row's Browse button opens that folder; anywhere else on
// the row header expands its coverage.
dashboardEl('folderDetail').addEventListener('click', function(event) {
  var browse = closestTarget(event, '[data-browse-folder]');
  if (browse) {
    openBrowse({folder_id: Number(browse.dataset.browseFolder)});
    return;
  }
  var head = closestTarget(event, '[data-folder-cov]');
  if (head) toggleFolderCov(head.dataset.folderCov || '', head);
});

// Attention cards' action buttons.
/** @type {Record<string, () => unknown>} */
var ATTENTION_ACTIONS = {
  'process-classify': function() { openProcess('classify'); },
  'process-previews': function() { openProcess('previews'); },
  'browse-unlocated': function() { openBrowse({location_status: 'none'}); },
  'sync': function() { return startDashboardSync(); },
  'duplicates': function() { window.location.href = '/duplicates'; },
};
dashboardEl('attentionGrid').addEventListener('click', function(event) {
  var button = closestTarget(event, '[data-attention-action]');
  var action = button && ATTENTION_ACTIONS[button.dataset.attentionAction || ''];
  if (action) action();
});

document.addEventListener('DOMContentLoaded', bootstrapDashboard);
