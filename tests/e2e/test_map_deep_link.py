from playwright.sync_api import expect

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


def _stub_leaflet(route):
    url = route.request.url
    if url.endswith(".css"):
        route.fulfill(status=200, content_type="text/css", body="")
        return
    route.fulfill(status=200, content_type="application/javascript", body=LEAFLET_STUB)


def test_map_photo_id_deep_link_focuses_marker(live_server, page):
    """The map page zooms to and opens the marker named by ?photo_id=."""
    pid = live_server["data"]["photos"][0]
    live_server["db"].conn.execute(
        "UPDATE photos SET latitude = ?, longitude = ? WHERE id = ?",
        (37.7749, -122.4194, pid),
    )
    live_server["db"].conn.commit()

    page.route("https://unpkg.com/**", _stub_leaflet)
    page.goto(f"{live_server['url']}/map?photo_id={pid}")

    page.wait_for_function(
        "pid => window.activePhotoId === pid && !!window.__openedPopup",
        arg=pid,
        timeout=3000,
    )
    active_card = page.locator(f".sidebar-card.active[data-id='{pid}']")
    expect(active_card).to_be_visible()
    expect(page.locator("#mapStatus")).to_contain_text("Showing 1 of 1 geolocated photos")
    expect(page.locator("#mapStatus")).to_contain_text("map coverage")
    missing_link = page.locator("#mapStatus a")
    expect(missing_link).to_have_attribute("href", "/browse?location_status=none")
    expect(missing_link).to_contain_text("without coordinates")


def test_map_photo_id_deep_link_reports_missing_location(live_server, page):
    """A map deep link to an unplottable photo gives a targeted status."""
    pid = live_server["data"]["photos"][0]

    page.route("https://unpkg.com/**", _stub_leaflet)
    page.goto(f"{live_server['url']}/map?photo_id={pid}")

    expect(page.locator("#mapStatus")).to_contain_text("No map location found for this photo.")


def test_empty_map_links_to_photos_without_coordinates(live_server, page):
    """An all-empty map gives users a working path to the affected photos."""
    page.route("https://unpkg.com/**", _stub_leaflet)
    page.goto(f"{live_server['url']}/map")

    expect(page.locator("#mapStatus")).to_contain_text("No geolocated photos")
    missing_link = page.locator("#mapStatus a")
    expect(missing_link).to_have_attribute("href", "/browse?location_status=none")
    expect(missing_link).to_contain_text("without coordinates")


def test_map_photo_id_deep_link_is_one_shot_for_later_filters(live_server, page):
    """Later filter changes should not keep treating the deep-link photo as missing."""
    linked_pid = live_server["data"]["photos"][0]
    other_pid = live_server["data"]["photos"][3]
    live_server["db"].conn.execute(
        "UPDATE photos SET latitude = ?, longitude = ? WHERE id = ?",
        (37.7749, -122.4194, linked_pid),
    )
    live_server["db"].conn.execute(
        "UPDATE photos SET latitude = ?, longitude = ? WHERE id = ?",
        (40.7128, -74.0060, other_pid),
    )
    live_server["db"].conn.commit()

    page.route("https://unpkg.com/**", _stub_leaflet)
    page.goto(f"{live_server['url']}/map?photo_id={linked_pid}")
    page.wait_for_function(
        "pid => window.activePhotoId === pid && !!window.__openedPopup",
        arg=linked_pid,
        timeout=3000,
    )

    # Filter down to just the robin via the shared filter bar. The old
    # `#filterSpecies` select was removed when Map adopted the universal bar.
    page.wait_for_selector("#vireoFilterBar", timeout=5000)
    search = page.locator(".vf-search input")
    search.fill("robin")
    search.press("Enter")

    expect(page.locator("#mapStatus")).to_contain_text("Showing 1 of 2 geolocated photos")
    expect(page.locator("#mapStatus")).not_to_contain_text("No map location found")


def test_large_map_renders_in_bounded_batches_and_virtualizes_sidebar(
    live_server, page
):
    """Large libraries must not monopolize or exhaust the browser main thread."""
    photo_count = 2500
    matching_count = 5000
    photos = [
        {
            "id": index + 1,
            "filename": f"photo-{index + 1}.jpg",
            "latitude": 37.0 + (index % 100) / 1000,
            "longitude": -122.0 - (index % 100) / 1000,
            "timestamp": "2026-01-01T12:00:00",
            "rating": 0,
            "species": None,
            "coord_source": "exif",
            "keyword_location_name": None,
            "folder_id": 1,
            "edit_recipe": None,
        }
        for index in range(photo_count)
    ]
    payload = {
        "photos": photos,
        "total_filtered": matching_count,
        "total_rendered": photo_count,
        "render_limit": photo_count,
        "truncated": True,
        "total_photos": matching_count,
        "total_geolocated": matching_count,
        "total_without_coordinates": 0,
    }

    page.route("https://unpkg.com/**", _stub_leaflet)
    page.route("**/api/photos/geo**", lambda route: route.fulfill(json=payload))
    page.goto(f"{live_server['url']}/map")

    page.wait_for_function(
        "count => window.__mapMarkerCount === count",
        arg=photo_count,
        timeout=15_000,
    )
    expect(page.locator("#mapStatus")).to_contain_text(
        f"Showing {photo_count} of {matching_count} matching photos"
    )
    expect(page.locator("#mapStatus")).to_contain_text("refine the filters")

    batch_sizes = page.evaluate("window.__mapMarkerBatchSizes")
    assert len(batch_sizes) > 1
    assert max(batch_sizes) <= 100

    # The scroll area represents all results, but only the visible window and
    # a small overscan are materialized (and allowed to request thumbnails).
    rendered_cards = page.locator(".sidebar-card").count()
    assert rendered_cards < 50
    assert rendered_cards < photo_count

    page.locator("#sidebarList").evaluate("el => { el.scrollTop = el.scrollHeight; }")
    expect(page.locator(f".sidebar-card[data-id='{photo_count}']")).to_be_visible()
