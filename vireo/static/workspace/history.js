// The Recent Jobs table: the last ten jobs with their status, timing and a
// one-line result.
// ES module.

import { byId } from './state.js';
import { escapeHtml, formatDuration, safeFetch } from './shared.js';

/** A raw result object as "key: value" pairs, arrays and objects summarized. @param {unknown} raw */
function resultKeysText(raw) {
  try {
    var parsed = /** @type {Record<string, unknown>} */ (typeof raw === 'string' ? JSON.parse(raw) : raw);
    var parts = [];
    for (var k in parsed) {
      var v = parsed[k];
      // Arrays/objects (e.g. duplicate-scan's `proposals` list) would
      // stringify to `[object Object],...` and blow out the cell;
      // summarize them by length instead.
      var disp;
      if (Array.isArray(v)) disp = v.length + ' items';
      else if (v !== null && typeof v === 'object') disp = '{...}';
      else disp = String(v);
      parts.push(k + ': ' + disp);
    }
    return parts.join(', ');
  } catch(e) { return String(raw); }
}

/** A failed job's error, truncated to its first line, or null. @param {unknown} raw */
function resultErrorText(raw) {
  try {
    var parsed2 = /** @type {{error?: unknown} | null} */ (typeof raw === 'string' ? JSON.parse(raw) : raw);
    if (parsed2 && parsed2.error) {
      // Truncate long errors to first line
      var errMsg = String(parsed2.error).split('\n')[0] || '';
      if (errMsg.length > 100) errMsg = errMsg.substring(0, 100) + '...';
      return errMsg;
    }
  } catch(e2) {}
  return null;
}

export async function loadHistory() {
  try {
    var rows = /** @type {JobHistoryRow[]} */ (await safeFetch('/api/jobs/history?limit=10', {}, { toast: false }));
    if (rows.length === 0) {
      byId('historyContent').innerHTML = '<span style="color:var(--text-ghost);font-size:13px;">No jobs have run yet.</span>';
      return;
    }
    var html = '<table class="data-table-sm"><thead><tr><th>Type</th><th>Status</th><th>Started</th><th>Duration</th><th>Result</th></tr></thead><tbody>';
    rows.forEach(function(r) {
      var cls = r.status === 'completed' ? 'status-completed' : r.status === 'failed' ? 'status-failed' : '';
      var started = r.started_at ? window.VireoTime.dateTime(new Date(r.started_at)) : '-';
      var duration = r.duration != null ? formatDuration(r.duration) : '-';
      var result = '-';
      if (r.summary) {
        // The server's prose summary (job_summaries.py / step summaries)
        // already words a Process run's skipped stages as skips; the raw
        // key dump below would count those notes under "errors".
        result = r.summary;
      } else if (r.result) {
        result = resultKeysText(r.result);
      }
      if ((r.error_count || 0) > 0 && result === '-') result = '';
      // For failed jobs, show the error message
      if (r.result) result = resultErrorText(r.result) ?? result;
      // Vireo's own work (maintenance, recurring checks) says so.
      var startedBy = r.origin === 'system'
        ? ' <span style="color:var(--text-muted);" title="' + escapeHtml(r.reason || 'Started by Vireo') + '">(Vireo)</span>' : '';
      html += '<tr><td>' + escapeHtml(r.type) + startedBy + '</td><td class="' + cls + '">' + r.status + '</td>' +
        '<td>' + started + '</td><td>' + duration + '</td><td style="' + (r.status === 'failed' ? 'color:var(--danger);' : '') + '">' + escapeHtml(result) + '</td></tr>';
    });
    html += '</tbody></table>';
    byId('historyContent').innerHTML = html;
  } catch(e) {
    byId('historyContent').innerHTML = '<span style="color:var(--danger);font-size:13px;">Failed to load</span>';
  }
}
