// Page startup: the module the template loads. Binds the tabs, the action
// buttons and the delegated row buttons, then loads the summary and the
// Integrity tab.
// ES module.

import { byId } from './helpers.js';
import { bindTabs, loadSummary, runAllChecks } from './summary.js';
import { bindDriftTable, resolveAll, runDriftCheck } from './drift.js';
import { bindOrphanTable, removeAllOrphans, runOrphanCheck } from './orphans.js';
import { bindUntrackedTable, importAllUntracked, runUntrackedCheck, sendToPipeline } from './untracked.js';
import { bindSidecarTable, deleteAllSidecars, runSidecarCheck } from './sidecars.js';
import { bindIntegrityTable, loadIntegrity, runHashVerify } from './integrity.js';

/** @param {string} id @param {() => unknown} action */
function onClick(id, action) {
  byId(id).addEventListener('click', function() { action(); });
}

bindTabs();
onClick('btnRunAll', runAllChecks);
onClick('btnDrift', runDriftCheck);
onClick('btnSyncAllFromXmp', function() { return resolveAll('use_xmp'); });
onClick('btnSyncAllToXmp', function() { return resolveAll('use_db'); });
onClick('btnOrphans', runOrphanCheck);
onClick('btnRemoveAllOrphans', removeAllOrphans);
onClick('btnUntracked', runUntrackedCheck);
onClick('btnImportAllUntracked', importAllUntracked);
onClick('sendToPipelineBtn', sendToPipeline);
onClick('btnSidecars', runSidecarCheck);
onClick('btnDeleteAllSidecars', deleteAllSidecars);
onClick('btnVerify', runHashVerify);
bindDriftTable();
bindOrphanTable();
bindUntrackedTable();
bindSidecarTable();
bindIntegrityTable();

loadSummary();
loadIntegrity();
