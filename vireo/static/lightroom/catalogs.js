// Phase 1: the list of catalogs to import, the native file picker, and the
// phase switcher the other phases share.
// ES module.

import { byId, state } from './state.js';
import { escapeHtml, pickFile } from './shared.js';

export function addCatalog() {
  var input = /** @type {HTMLInputElement} */ (byId('catalogInput'));
  var path = input.value.trim();
  if (!path || state.catalogs.indexOf(path) !== -1) return;
  state.catalogs.push(path);
  input.value = '';
  renderCatalogs();
}

/** @param {number} idx */
export function removeCatalog(idx) {
  state.catalogs.splice(idx, 1);
  renderCatalogs();
}

export function renderCatalogs() {
  var html = '';
  state.catalogs.forEach(function(c, i) {
    html += '<div class="catalog-item">' +
      '<span class="remove-btn" data-catalog-index="' + i + '">&times;</span>' +
      '<span>' + escapeHtml(c) + '</span></div>';
  });
  byId('catalogList').innerHTML = html;
  /** @type {HTMLButtonElement} */ (byId('lrPreviewBtn')).disabled = state.catalogs.length === 0;
}

/** The catalog list's remove buttons, delegated so re-rendering keeps them bound. */
export function bindCatalogList() {
  byId('catalogList').addEventListener('click', function(event) {
    var target = event.target instanceof Element ? event.target.closest('[data-catalog-index]') : null;
    if (target) removeCatalog(Number(target.getAttribute('data-catalog-index')));
  });
}

/** @param {number} n */
export function lrShowPhase(n) {
  document.querySelectorAll('.lr-phase').forEach(function(el) { el.classList.remove('active'); });
  byId('lrPhase' + n).classList.add('active');
}

export async function browseForCatalog() {
  var path = await pickFile({
    title: 'Select Lightroom Catalog',
    filters: [{ name: 'Lightroom Catalog', extensions: ['lrcat'] }],
  });
  if (path) {
    /** @type {HTMLInputElement} */ (byId('catalogInput')).value = path;
  }
}
