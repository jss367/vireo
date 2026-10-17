/* Leaflet must observe panel-driven container changes as well as window resize. */
/** @param {string} containerId @param {Record<string, unknown>} options */
function createMapWithContainerResize(containerId, options) {
  var map = L.map(containerId, options);
  var observer = new ResizeObserver(function() {
    map.invalidateSize({ pan: false, debounceMoveend: true });
  });
  var container = document.getElementById(containerId);
  if (!container) throw new Error('Missing map container: ' + containerId);
  observer.observe(container);
  return map;
}
