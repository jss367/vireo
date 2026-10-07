"""Deterministic Leaflet replacement for map-page browser tests.

The application intentionally loads Leaflet from unpkg, but E2E coverage
should not depend on third-party network availability.
"""

LEAFLET_STUB = """
window.L = {
  tileLayer: function() {
    return { addTo: function() { return this; } };
  },
  map: function() {
    return {
      setView: function(latlng, zoom) {
        window.__lastMapSetView = { latlng: latlng, zoom: zoom };
        return this;
      },
      addLayer: function() { return this; },
      invalidateSize: function() { return this; },
      fitBounds: function(bounds) {
        window.__lastFitBounds = bounds;
        return this;
      },
      getZoom: function() { return 2; }
    };
  },
  control: {
    layers: function() {
      return { addTo: function() { return this; } };
    }
  },
  markerClusterGroup: function() {
    var layers = [];
    window.__mapMarkerBatchSizes = [];
    return {
      addLayer: function(marker) {
        layers.push(marker);
        window.__mapMarkerCount = layers.length;
        return this;
      },
      addLayers: function(markers) {
        window.__mapMarkerBatchSizes.push(markers.length);
        layers = layers.concat(markers);
        window.__mapMarkerCount = layers.length;
        return this;
      },
      clearLayers: function() {
        layers = [];
        window.__mapMarkerCount = 0;
        return this;
      },
      getBounds: function() { return [[0, 0], [1, 1]]; },
      zoomToShowLayer: function(marker, cb) {
        window.__zoomedToMarker = marker.getLatLng();
        if (cb) cb();
      },
      on: function() { return this; }
    };
  },
  divIcon: function(opts) { return opts; },
  marker: function(latlng) {
    return {
      _latlng: latlng,
      bindPopup: function(popup) { this._popup = popup; return this; },
      on: function(name, handler) {
        this._handlers = this._handlers || {};
        this._handlers[name] = handler;
        return this;
      },
      getLatLng: function() { return this._latlng; },
      openPopup: function() {
        window.__openedPopup = this._popup;
        return this;
      }
    };
  },
  Control: {
    extend: function(definition) {
      function Control() {}
      Control.prototype.addTo = function(map) {
        this._div = definition.onAdd.call(this, map);
        return this;
      };
      Object.keys(definition).forEach(function(key) {
        if (key !== "onAdd") Control.prototype[key] = definition[key];
      });
      return Control;
    }
  },
  DomUtil: {
    create: function(tag, className) {
      var el = document.createElement(tag);
      el.className = className;
      return el;
    }
  },
  DomEvent: {
    disableClickPropagation: function() {},
    disableScrollPropagation: function() {}
  }
};
"""


def stub_leaflet(route):
    """Fulfill unpkg Leaflet JS/CSS requests without external network."""
    if route.request.url.endswith(".css"):
        route.fulfill(status=200, content_type="text/css", body="")
        return
    route.fulfill(
        status=200,
        content_type="application/javascript",
        body=LEAFLET_STUB,
    )
