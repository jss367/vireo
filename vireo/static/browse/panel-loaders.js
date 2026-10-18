/* Typed boundary between the legacy renderers and the panel request lifecycle. */
import { state } from './state.js';
import { VireoPanelLoader, safeFetch } from './shared.js';
import { panelRequests } from './panel-requests.js';
import { renderDetailPredictions, renderSelectionPredictions } from './prediction-panels.js';
import { selectionPanel } from './selection-panel-state.js';
import { renderSelectionKeywordSuggestions } from './selection-panel.js';
import { getBrowseSelection, selectionIdsKey } from './selection.js';

/** @param {readonly number[]} ids */
function acceptsSelection(ids) { return ids.length > 1 && ids.length <= 1000; }
/** @param {readonly number[]} ids */
function isActiveSelection(ids) {
  const selected = getBrowseSelection();
  return ids.length === selected.length && selectionIdsKey(ids) === selectionIdsKey(selected);
}
/** @param {readonly number[]} ids */
function snapshot(ids) { return ids.slice(); }

/** @type {BrowsePanelLoaders} */
export var panelLoaders = {
  keywords: VireoPanelLoader.create({
    panel: /** @type {HTMLElement} */ (document.getElementById('selectionKeywordSuggestions')),
    lane: panelRequests.keywords,
    accepts: acceptsSelection, key: selectionIdsKey, snapshot: snapshot, isActive: isActiveSelection,
    reset: function() { selectionPanel.keywords.reset(); },
    request: /** @returns {Promise<SelectionKeywordResponse>} */ function(ids) {
      return safeFetch('/api/selection/keyword-suggestions', {
        method: 'POST', headers: {'Content-Type': 'application/json'}, body: JSON.stringify({photo_ids: ids}),
      }, {toast: false});
    },
    render: function(data, ids) { renderSelectionKeywordSuggestions(data.keywords || [], data.selected_count ?? ids.length); },
    loadingMessage: 'Checking selected keywords...',
    errorMessage: 'Could not load keyword suggestions.',
    retryLabel: 'Retry loading keyword suggestions',
  }),
  predictions: VireoPanelLoader.create({
    panel: /** @type {HTMLElement} */ (document.getElementById('selectionPredictions')),
    lane: panelRequests.predictions,
    accepts: acceptsSelection, key: selectionIdsKey, snapshot: snapshot, isActive: isActiveSelection,
    reset: function() { selectionPanel.predictions.reset(); },
    request: /** @returns {Promise<SelectionPredictionResponse>} */ function(ids) {
      return safeFetch('/api/selection/prediction-suggestions', {
        method: 'POST', headers: {'Content-Type': 'application/json'}, body: JSON.stringify({photo_ids: ids}),
      }, {toast: false});
    },
    render: function(data, ids) { renderSelectionPredictions(data.predictions || [], data.selected_count ?? ids.length, data); },
    loadingMessage: 'Checking predictions...',
    errorMessage: 'Could not load predictions.',
    retryLabel: 'Retry loading selection predictions',
  }),
  detailPredictions: VireoPanelLoader.create({
    panel: /** @type {HTMLElement} */ (document.getElementById('detailPredictions')),
    lane: panelRequests.detailPredictions,
    accepts: function(/** @type {number} */ id) { return Number.isInteger(id) && id > 0; },
    key: function() { return undefined; },
    snapshot: function(id) { return id; },
    isActive: function(id) { return state.detailPhotoId === id; },
    reset: function() {},
    request: /** @returns {Promise<DetailPredictionsResponse>} */ function(id) {
      return safeFetch('/api/predictions?photo_ids=' + encodeURIComponent(id), {}, {toast: false});
    },
    render: function(data, id) { renderDetailPredictions(data, id); },
    loadingMessage: 'Loading predictions...',
    errorMessage: 'Could not load predictions.',
    retryLabel: 'Retry loading photo predictions',
  }),
};
