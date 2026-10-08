/* One listener survives every selection-panel repaint. No row data or
   executable handlers are interpolated into button attributes. */
(function(global) {
  'use strict';

  var bound = new WeakSet();

  global.Vireo.browse.selectionPanel.bindActions = function() {
    var panel = document.getElementById('selectionPanel');
    if (!panel || bound.has(panel)) return;
    bound.add(panel);
    panel.addEventListener('click', function(event) {
      var button = event.target.closest('button[data-selection-action]');
      if (!button || !panel.contains(button) || button.disabled) return;
      var action = button.getAttribute('data-selection-action');
      if (action === 'edit') {
        openBatchDevelopmentEditor();
      } else if (action === 'paste') {
        pasteEditSettingsToSelection();
      } else if (action === 'wildlife-exclude' || action === 'wildlife-include') {
        setSelectionWildlifeExcluded(action === 'wildlife-exclude');
      } else if (action === 'keyword-add' || action === 'keyword-remove') {
        var keywordId = Number(button.getAttribute('data-keyword-id'));
        if (!Number.isInteger(keywordId) || keywordId <= 0) return;
        if (action === 'keyword-add') applySelectionKeyword(keywordId);
        else removeSelectionKeyword(keywordId);
      } else if (action === 'prediction-toggle') {
        toggleSelectionPredictions();
      } else {
        var indexAttr = button.getAttribute('data-prediction-row');
        if (indexAttr === null || indexAttr === '') return;
        var index = Number(indexAttr);
        var row = global.Vireo.browse.selectionPanel.predictions.getRow(index);
        if (!row) return;
        if (action === 'prediction-accept' || action === 'prediction-accept-all') {
          acceptSelectionPrediction(index, action === 'prediction-accept-all', button);
        } else if (action === 'prediction-show') {
          showSelectionPredictionPhotos(index, button);
        } else if (action === 'prediction-review' && row.reviewPhotoId != null) {
          openPredictionInReview(row.reviewPhotoId);
        }
      }
    });
  };
})(window);
