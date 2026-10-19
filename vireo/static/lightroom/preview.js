// Phase 2: the per-catalog preview and the cross-catalog conflicts.
// ES module.

import { byId, state } from './state.js';
import { escapeHtml, safeFetch } from './shared.js';
import { lrShowPhase } from './catalogs.js';

/** @param {LightroomPreview} data */
function renderPreview(data) {
  var html = '';
  (data.catalogs || []).forEach(function(c) {
    html += '<tr><td>' + escapeHtml(c.catalog) + '</td><td>' + c.total_files + '</td>' +
      '<td>' + c.matched_files + '</td><td>' + c.unmatched_files + '</td>' +
      '<td>' + c.keyword_count + '</td></tr>';
  });
  byId('previewBody').innerHTML = html;

  if (data.conflict_count > 0) {
    byId('conflictSection').style.display = '';
    byId('conflictCount').textContent = String(data.conflict_count);
    var chtml = '';
    (data.conflicts || []).slice(0, 50).forEach(function(c) {
      chtml += '<div class="conflict-item"><div class="conflict-file">' + escapeHtml(c.file_path) + '</div>';
      for (var cat in c.keywords_by_catalog) {
        chtml += '<div><span class="conflict-cat">' + escapeHtml(cat) + ':</span> ' +
          escapeHtml((c.keywords_by_catalog[cat] || []).join(', ')) + '</div>';
      }
      chtml += '</div>';
    });
    byId('conflictList').innerHTML = chtml;
  } else {
    byId('conflictSection').style.display = 'none';
  }
}

export async function previewImport() {
  var status = byId('lrPreviewStatus');
  var previewBtn = /** @type {HTMLButtonElement} */ (byId('lrPreviewBtn'));
  status.textContent = 'Loading preview...';
  previewBtn.disabled = true;
  try {
    var data = /** @type {LightroomPreview} */ (await safeFetch('/api/import/preview', {
      method: 'POST', headers: {'Content-Type': 'application/json'},
      body: JSON.stringify({catalogs: state.catalogs}),
    }, { toast: false }));
    renderPreview(data);
    lrShowPhase(2);
    status.textContent = '';
  } catch(e) {
    status.textContent = 'Error: ' + (e instanceof Error ? e.message : String(e));
  }
  previewBtn.disabled = state.catalogs.length === 0;
}
