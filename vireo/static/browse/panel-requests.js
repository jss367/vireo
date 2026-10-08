/* Browse panel request ownership. Each lane keeps its cache and active request
   private; an older response must never render or clear a newer request's key.
   Load before the panel controllers. This does not cancel server-side work. */
(function(global) {
  'use strict';

  function createLane() {
    var owner = {};
    var cachedKey;

    function observe() {
      var observedOwner = owner;
      return function() { return owner === observedOwner; };
    }

    function invalidate() {
      owner = {};
      cachedKey = undefined;
    }

    return {
      // A keyed request is reused until invalidation or failure. Unkeyed
      // requests always supersede their predecessor (e.g. another Show click).
      begin: function(key) {
        if (key !== undefined && key === cachedKey) return null;
        invalidate();
        cachedKey = key;
        var isCurrent = observe();
        return {
          isCurrent: isCurrent,
          fail: function() {
            if (!isCurrent()) return false;
            invalidate();
            return true;
          },
        };
      },
      observe: observe,
      invalidate: invalidate,
    };
  }

  var Vireo = global.Vireo = global.Vireo || {};
  Vireo.browse = Vireo.browse || {};
  Vireo.browse.panelRequests = {
    keywords: createLane(),
    predictions: createLane(),
    detailPredictions: createLane(),
    predictionPhotos: createLane(),
    wildlife: createLane(),
  };
})(window);
