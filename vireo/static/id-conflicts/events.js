// The delegated listeners for markup the page renders. The sort, exclude and
// filter chips, the table's checkboxes, thumbnails and prediction buttons
// carry data attributes instead of inline handlers, so one listener per event
// on the document survives every re-render.

import { predictionAction, toggleAllRows, toggleRow } from './decisions.js';
import { setFilter, setSort, toggleExclude } from './render.js';
import { openCompareLightbox } from './table.js';

/** @param {MouseEvent} event */
function onCompareClick(event) {
  if (!(event.target instanceof Element)) return;
  var button = event.target.closest(/** @type {'button'} */ (
    'button[data-compare-sort], button[data-compare-exclude], button[data-compare-filter], ' +
    'button[data-compare-open], button[data-prediction-action]'));
  if (!button) return;
  var data = button.dataset;
  if (data.compareSort !== undefined) setSort(data.compareSort);
  else if (data.compareExclude !== undefined) toggleExclude(data.compareExclude);
  else if (data.compareFilter !== undefined) setFilter(data.compareFilter);
  else if (data.compareOpen !== undefined) openCompareLightbox(Number(data.compareOpen));
  else predictionAction(Number(data.predictionId), data.predictionAction || '');
}

/** @param {Event} event */
function onCompareChange(event) {
  var input = event.target;
  if (!(input instanceof HTMLInputElement) || input.type !== 'checkbox') return;
  if (input.hasAttribute('data-compare-select-all')) toggleAllRows(input.checked);
  else if (input.dataset.compareRow !== undefined) toggleRow(Number(input.dataset.compareRow), input.checked);
}

export function bindCompareRenderedMarkup() {
  document.addEventListener('click', onCompareClick);
  document.addEventListener('change', onCompareChange);
}
