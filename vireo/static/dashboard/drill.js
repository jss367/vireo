// Bar rows that open the matching photos in Browse (data-dashboard-drill).
// ES module.

import { escapeAttr } from './shared.js';
import { openBrowse } from './scope.js';

/** @param {DashboardBrowseParams} params @param {string=} title */
export function drillAttrs(params, title) {
  return ' class="bar-row dashboard-drill" role="link" tabindex="0" data-dashboard-drill="' +
    escapeAttr(JSON.stringify(params || {})) + '" title="' +
    escapeAttr(title || 'Open matching photos in Browse') + '"';
}

/** @param {Event} event */
export function activateDashboardDrill(event) {
  var row = event.target instanceof Element ? event.target.closest('[data-dashboard-drill]') : null;
  if (!row) return;
  try { openBrowse(JSON.parse(row.getAttribute('data-dashboard-drill') || '')); }
  catch (_) { openBrowse(); }
}
