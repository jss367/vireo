/* Selection-panel row data stays private. Markup carries identifiers only;
   actions read immutable snapshots of the rows currently on screen. */
(function(global) {
  'use strict';

  var keywords = new Map();
  var predictionRows = [];
  var predictionData = null;
  var expanded = false;

  function ids(values) {
    return Object.freeze((values || []).slice());
  }

  var Vireo = global.Vireo = global.Vireo || {};
  Vireo.browse = Vireo.browse || {};
  Vireo.browse.selectionPanel = {
    keywords: {
      reset: function() { keywords.clear(); },
      replace: function(rows) {
        keywords.clear();
        rows.forEach(function(row) {
          keywords.set(Number(row.id), Object.freeze({
            name: row.name,
            missingPhotoIds: ids(row.missing_photo_ids),
            presentPhotoIds: ids(row.present_photo_ids),
          }));
        });
      },
      get: function(id) { return keywords.get(Number(id)); },
    },
    predictions: {
      // Expansion is a viewing preference; keep it across selection changes,
      // but retire both the old toggle payload and every actionable row.
      reset: function() {
        predictionRows = [];
        predictionData = null;
      },
      remember: function(predictions, selectedCount, meta) {
        predictionData = {predictions: predictions, selectedCount: selectedCount, meta: meta};
      },
      isExpanded: function() { return expanded; },
      toggle: function() {
        expanded = !expanded;
        return predictionData;
      },
      replaceRows: function(rows) {
        predictionRows = rows.map(function(row) {
          return Object.freeze({
            species: row.species,
            acceptableIds: ids(row.acceptable_prediction_ids),
            photoIds: ids(row.predicted_photo_ids),
            reviewPhotoId: (row.ambiguous_photo_ids || [])[0],
          });
        });
      },
      getRow: function(index) {
        return Number.isInteger(index) && index >= 0 ? predictionRows[index] : undefined;
      },
    },
  };
})(window);
