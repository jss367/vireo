// Rendering log records, the level and text filter, and Clear.
// ES module.

import { LEVEL_ORDER, state } from './state.js';
import { VireoTextSearch, escapeHtml } from './shared.js';

/** @param {string} id */
function byId(id) {
  return /** @type {HTMLElement} */ (document.getElementById(id));
}

/** @param {LogRecord} record */
export function addLogLine(record) {
  var time = new Date(record.time * 1000);
  var timeStr = window.VireoTime.time(time, { hour: '2-digit', minute: '2-digit', second: '2-digit' });
  state.lineCount += 1;
  var container = byId('logContainer');
  var div = document.createElement('div');
  div.className = 'log-line level-' + record.level;
  div.dataset.level = record.level;
  div.dataset.message = record.message || '';
  div.innerHTML = '<span class="log-time">' + timeStr + '</span>' +
    '<span class="log-level">' + record.level + '</span>' +
    '<span class="log-logger">' + escapeHtml(record.logger || '') + '</span>' +
    escapeHtml(record.message || '');
  container.appendChild(div);

  if (/** @type {HTMLInputElement} */ (byId('autoScroll')).checked) {
    container.scrollTop = container.scrollHeight;
  }
  byId('logCount').textContent = state.lineCount + ' lines';
}

export function applyFilter() {
  var minLevel = /** @type {HTMLSelectElement} */ (byId('levelFilter')).value;
  var search = /** @type {HTMLInputElement} */ (byId('searchFilter')).value.trim();
  var searchOptions = VireoTextSearch.readOptions('logSearch');
  var minOrder = LEVEL_ORDER[minLevel] || 0;
  document.querySelectorAll('.log-line').forEach(function(node) {
    var el = /** @type {HTMLElement} */ (node);
    var levelOk = (LEVEL_ORDER[el.dataset.level || ''] || 0) >= minOrder;
    var searchOk = !search || VireoTextSearch.matchesFields(
      el.dataset.message || '',
      search,
      searchOptions
    );
    el.classList.toggle('filtered', !(levelOk && searchOk));
  });
}

export function clearLogs() {
  state.lineCount = 0;
  byId('logContainer').innerHTML = '';
  byId('logCount').textContent = '0 lines';
}
