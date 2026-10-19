// Phase 3: running the import job, following its progress stream, and Done.
// ES module.

import { byId, state } from './state.js';
import { safeEventSource, safeFetch } from './shared.js';
import { lrShowPhase, renderCatalogs } from './catalogs.js';

/** @param {LightroomJobComplete} result @param {HTMLElement} status */
function showImportResult(result, status) {
  if (result.status === 'completed' && result.result) {
    status.textContent = 'Complete' + (result.duration ? ' (' + result.duration + 's)' : '');
    var summary = byId('lrResult');
    summary.style.display = '';
    summary.innerHTML =
      '<span class="ok">Imported: ' + (result.result.imported || 0) + '</span><br>' +
      '<span class="skip">Skipped: ' + (result.result.skipped || 0) + '</span><br>' +
      '<span class="fail">Failed: ' + (result.result.failed || 0) + '</span>';
  } else {
    status.textContent = 'Failed: ' + (result.errors || []).join(', ');
  }
  byId('lrDoneBtn').style.display = '';
}

export async function executeLrImport() {
  lrShowPhase(3);
  var fill = byId('lrProgressFill');
  var status = byId('lrStatus');
  fill.style.width = '0%';
  status.textContent = 'Starting import...';

  var checked = /** @type {HTMLInputElement} */ (document.querySelector('input[name=strategy]:checked'));
  var strategy = checked.value;
  var writeXmp = /** @type {HTMLInputElement} */ (byId('writeXmp')).checked;

  try {
    var data = /** @type {{job_id: string}} */ (await safeFetch('/api/jobs/import', {
      method: 'POST', headers: {'Content-Type': 'application/json'},
      body: JSON.stringify({ catalogs: state.catalogs, strategy: strategy, write_xmp: writeXmp }),
    }, { toast: false }));
    status.textContent = 'Importing...';

    safeEventSource('/api/jobs/' + data.job_id + '/stream', {
      onProgress: function(p) {
        if (p.total > 0) {
          fill.style.width = Math.round((p.current / p.total) * 100) + '%';
          status.textContent = p.current.toLocaleString() + ' / ' + p.total.toLocaleString() + ' files';
        }
      },
      onComplete: function(result) {
        fill.style.width = '100%';
        showImportResult(result, status);
      },
      onError: function() {
        status.textContent = 'Connection lost.';
      }
    });
  } catch(e) {
    status.textContent = 'Error: ' + (e instanceof Error ? e.message : String(e));
  }
}

export function lrReset() {
  state.catalogs = [];
  renderCatalogs();
  byId('lrProgressFill').style.width = '0';
  byId('lrResult').style.display = 'none';
  byId('lrDoneBtn').style.display = 'none';
  lrShowPhase(1);
}
