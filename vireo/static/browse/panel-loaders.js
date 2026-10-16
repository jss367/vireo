/* Typed boundary between the legacy renderers and the panel request lifecycle.
   Load after selection-panel.js and prediction-panels.js, before boot.js. */
(function(/** @type {Window} */ global) {
  'use strict';
  const browse = global.Vireo?.browse;
  const lanes = browse?.panelRequests;
  const state = browse?.selectionPanel;
  if (!browse || !lanes || !state) return;

  /** @param {readonly number[]} ids */
  function acceptsSelection(ids) { return ids.length > 1 && ids.length <= 1000; }
  /** @param {readonly number[]} ids */
  function isActiveSelection(ids) {
    const selected = getBrowseSelection();
    return ids.length === selected.length && selectionIdsKey(ids) === selectionIdsKey(selected);
  }
  /** @param {readonly number[]} ids */
  function snapshot(ids) { return ids.slice(); }

  browse.panelLoaders = {
    keywords: global.VireoPanelLoader.create({
      panel: document.getElementById('selectionKeywordSuggestions'),
      lane: lanes.keywords,
      accepts: acceptsSelection, key: selectionIdsKey, snapshot: snapshot, isActive: isActiveSelection,
      reset: function() { state.keywords.reset(); },
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
    predictions: global.VireoPanelLoader.create({
      panel: document.getElementById('selectionPredictions'),
      lane: lanes.predictions,
      accepts: acceptsSelection, key: selectionIdsKey, snapshot: snapshot, isActive: isActiveSelection,
      reset: function() { state.predictions.reset(); },
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
    detailPredictions: global.VireoPanelLoader.create({
      panel: document.getElementById('detailPredictions'),
      lane: lanes.detailPredictions,
      accepts: function(/** @type {number} */ id) { return Number.isInteger(id) && id > 0; },
      key: function() { return undefined; },
      snapshot: function(id) { return id; },
      isActive: function(id) { return global._detailPhotoId === id; },
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
})(window);
