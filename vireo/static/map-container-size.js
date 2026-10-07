/* Leaflet must observe panel-driven container changes as well as window resize. */
function createMapWithContainerResize(containerId, options) {
  var map = L.map(containerId, options);
  var observer = new ResizeObserver(function() {
    map.invalidateSize({ pan: false, debounceMoveend: true });
  });
  observer.observe(document.getElementById(containerId));
  return map;
}
