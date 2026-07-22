import json
from urllib.parse import parse_qs, urlparse

import pytest
from playwright.sync_api import expect

LEAFLET_STUB = """
window.__locationReviewLeafletMarkers = [];
window.L = {
  tileLayer: function() { return {}; },
  map: function() {
    return {
      setView: function() { return this; },
      fitBounds: function() { return this; },
      removeLayer: function() { return this; },
      invalidateSize: function() { return this; }
    };
  },
  control: {
    layers: function() { return { addTo: function() { return this; } }; }
  },
  divIcon: function(options) { return options; },
  marker: function(latlng) {
    var handlers = {};
    var marker = {
      latlng: latlng,
      addTo: function() { return this; },
      bindTooltip: function() { return this; },
      on: function(name, handler) { handlers[name] = handler; return this; },
      fire: function(name) {
        if (handlers[name]) handlers[name]({target: this});
        return this;
      }
    };
    window.__locationReviewLeafletMarkers.push(marker);
    return marker;
  }
};
"""

GOOGLE_MAPS_STUB = """
(function() {
  function LatLng(lat, lng) {
    this.lat = function() { return lat; };
    this.lng = function() { return lng; };
  }
  function Map() {
    this.fitBounds = function() {};
    this.getZoom = function() { return 14; };
    this.setZoom = function() {};
    this.addListener = function() {};
  }
  function Marker() {
    this.addListener = function() {};
    this.setMap = function() {};
  }
  function PlacesService() {
    this.nearbySearch = function(request, callback) {
      window.__locationReviewNearbyRequests = window.__locationReviewNearbyRequests || [];
      window.__locationReviewNearbyRequests.push({
        type: request.type || null,
        keyword: request.keyword || null
      });
      if (request.type === 'park') {
        callback([{
          place_id: 'nearby-park',
          name: 'Nearby Park',
          types: ['park'],
          vicinity: '100 Nearby Street',
          geometry: {location: new LatLng(32.751, -117.001)}
        }], 'OK');
      } else if (request.type === 'campground' && request.location.lat() > 33) {
        callback([{
          place_id: 'tamarisk-grove-campground',
          name: 'Tamarisk Grove Campground',
          types: ['campground', 'park'],
          vicinity: 'Yaqui Pass Road',
          geometry: {location: new LatLng(33.255, -116.405)}
        }], 'OK');
      } else if (!request.type && !request.keyword) {
        callback([{
          place_id: 'nearby-general-store',
          name: 'Nearby General Store',
          types: ['store'],
          vicinity: '1 Main Street',
          geometry: {location: request.location}
        }], 'OK');
      } else {
        callback([], 'ZERO_RESULTS');
      }
    };
  }
  function Geocoder() {
    this.geocode = function(request, callback) { callback([], 'ZERO_RESULTS'); };
  }
  function Autocomplete() {
    this.bindTo = function() {};
    this.addListener = function() {};
    this.getPlace = function() { return {}; };
  }
  window.google = {maps: {
    Map: Map,
    Marker: Marker,
    LatLng: LatLng,
    LatLngBounds: function() { this.extend = function() {}; },
    Geocoder: Geocoder,
    SymbolPath: {CIRCLE: 'circle'},
    MapTypeControlStyle: {HORIZONTAL_BAR: 'horizontal'},
    event: {addListenerOnce: function(map, event, callback) { callback(); }},
    places: {
      PlacesService: PlacesService,
      PlacesServiceStatus: {OK: 'OK'},
      Autocomplete: Autocomplete
    }
  }};
  window._vireoLocationReviewMapsReady();
})();
"""


def _stub_leaflet(route):
    if route.request.url.endswith(".css"):
        route.fulfill(status=200, content_type="text/css", body="")
    else:
        route.fulfill(
            status=200,
            content_type="application/javascript",
            body=LEAFLET_STUB,
        )


def test_location_review_is_a_navigable_collection_page(live_server, page):
    """The standalone page lets the user choose a collection and start its queue."""
    photo_id = live_server["data"]["photos"][0]
    with live_server["db"].conn:
        live_server["db"].conn.execute(
            "UPDATE photos SET latitude = ?, longitude = ? WHERE id = ?",
            (33.2550, -116.4050, photo_id),
        )
    collection_id = live_server["db"].add_collection(
        "San Diego Field Notes",
        json.dumps([{"field": "photo_ids", "value": [photo_id]}]),
    )

    page.route("https://unpkg.com/**", _stub_leaflet)
    page.goto(f"{live_server['url']}/locations/review")

    expect(page.locator("#locationReviewEmptyTitle")).to_have_text(
        "Choose a collection"
    )
    expect(page.locator("#locationReviewCollection")).to_contain_text(
        "San Diego Field Notes (1)"
    )
    expect(
        page.locator('.nav-tab[data-nav-id="location_review"]')
    ).to_have_class("nav-tab is-ephemeral active")

    page.locator("#locationReviewCollection").select_option(str(collection_id))
    page.wait_for_url(f"**/locations/review?collection_id={collection_id}")

    expect(page.locator("#locationReviewCollection")).to_have_value(
        str(collection_id)
    )
    expect(page.locator("#locationReviewGroupTitle")).to_have_text("1 photo")


def test_location_review_assigns_a_custom_name_to_coordinate_group(
    live_server, page,
):
    """The full-page queue maps a coordinate group and saves its chosen name."""
    photo_ids = live_server["data"]["photos"][:2]
    with live_server["db"].conn:
        live_server["db"].conn.executemany(
            "UPDATE photos SET latitude = ?, longitude = ? WHERE id = ?",
            [
                (33.2550, -116.4050, photo_ids[0]),
                (33.2553, -116.4052, photo_ids[1]),
            ],
        )

    page.route("https://unpkg.com/**", _stub_leaflet)
    page.goto(f"{live_server['url']}/browse")
    page.evaluate(
        "ids => sessionStorage.setItem('vireoLocationReviewSource', "
        "JSON.stringify({photo_ids: ids}))",
        photo_ids,
    )
    page.goto(f"{live_server['url']}/locations/review?source=selection")

    expect(page.locator("#locationReviewGroupTitle")).to_have_text("2 photos")
    expect(page.locator("#locationReviewScope")).to_contain_text(
        "2 photos ready for review"
    )
    expect(page.locator("#locationReviewCoordinates")).to_contain_text(
        "original photo coordinates"
    )

    page.locator("#locationReviewSearch").fill(
        "Anza-Borrego Desert State Park"
    )
    page.locator("#locationReviewCustom").click()
    expect(page.locator("#locationReviewAssign")).to_contain_text(
        "Assign “Anza-Borrego Desert State Park”"
    )
    page.locator("#locationReviewAssign").click()

    expect(page.locator("#locationReviewEmptyTitle")).to_have_text(
        "All locations reviewed"
    )
    rows = live_server["db"].conn.execute(
        "SELECT pk.photo_id, k.name, k.latitude, k.longitude "
        "FROM photo_keywords pk "
        "JOIN keywords k ON k.id = pk.keyword_id "
        "WHERE pk.photo_id IN (?, ?) AND k.type = 'location' "
        "ORDER BY pk.photo_id",
        photo_ids,
    ).fetchall()
    assert [
        (row["photo_id"], row["name"], row["latitude"], row["longitude"])
        for row in rows
    ] == [
        (photo_ids[0], "Anza-Borrego Desert State Park", 33.25515, -116.4051),
        (photo_ids[1], "Anza-Borrego Desert State Park", 33.25515, -116.4051),
    ]


def test_location_review_ranks_saved_and_google_places_by_distance(
    live_server, page,
):
    """A far saved place must not outrank a nearby Google suggestion."""
    target_id, saved_photo_id = live_server["data"]["photos"][:2]
    with live_server["db"].conn:
        live_server["db"].conn.execute(
            "UPDATE photos SET latitude = ?, longitude = ? WHERE id = ?",
            (32.750, -117.000, target_id),
        )
        saved_id = live_server["db"]._upsert_one_keyword(
            "Far Saved Place", None, latitude=32.938, longitude=-117.000,
        )
        live_server["db"].conn.execute(
            "INSERT INTO photo_keywords (photo_id, keyword_id) VALUES (?, ?)",
            (saved_photo_id, saved_id),
        )

    page.route(
        "**/api/config",
        lambda route: route.fulfill(json={"google_maps_api_key": "test-key"}),
    )
    page.route(
        "https://maps.googleapis.com/maps/api/js**",
        lambda route: route.fulfill(
            status=200,
            content_type="application/javascript",
            body=GOOGLE_MAPS_STUB,
        ),
    )
    page.goto(f"{live_server['url']}/browse")
    page.evaluate(
        "photoId => sessionStorage.setItem('vireoLocationReviewSource', "
        "JSON.stringify({photo_ids: [photoId]}))",
        target_id,
    )
    page.goto(f"{live_server['url']}/locations/review?source=selection")

    candidates = page.locator(".location-review-candidate")
    expect(candidates).to_have_count(2)
    expect(candidates.first).to_contain_text("Nearby Park")
    saved_candidate = candidates.filter(has_text="Far Saved Place")
    expect(saved_candidate.locator(".location-review-candidate-badge")).to_have_text(
        "Previously used"
    )
    expect(saved_candidate.locator(".location-review-candidate-meta")).not_to_contain_text(
        "Saved location"
    )


def test_location_review_suggests_campgrounds_and_can_show_all_nearby_places(
    live_server, page,
):
    """Focused suggestions include campgrounds; the toggle broadens categories."""
    photo_id = live_server["data"]["photos"][0]
    with live_server["db"].conn:
        live_server["db"].conn.execute(
            "UPDATE photos SET latitude = ?, longitude = ? WHERE id = ?",
            (33.2550, -116.4050, photo_id),
        )

    page.route(
        "**/api/config",
        lambda route: route.fulfill(json={"google_maps_api_key": "test-key"}),
    )
    page.route(
        "https://maps.googleapis.com/maps/api/js**",
        lambda route: route.fulfill(
            status=200,
            content_type="application/javascript",
            body=GOOGLE_MAPS_STUB,
        ),
    )
    page.goto(f"{live_server['url']}/browse")
    page.evaluate(
        "photoId => sessionStorage.setItem('vireoLocationReviewSource', "
        "JSON.stringify({photo_ids: [photoId]}))",
        photo_id,
    )
    page.goto(f"{live_server['url']}/locations/review?source=selection")

    nature_button = page.locator('[data-suggestion-mode="nature"]')
    all_button = page.locator('[data-suggestion-mode="all"]')
    expect(nature_button).to_have_attribute("aria-pressed", "true")
    campground = page.locator(
        ".location-review-candidate", has_text="Tamarisk Grove Campground"
    )
    expect(campground).to_be_visible()
    expect(campground.locator(".location-review-candidate-meta")).to_contain_text(
        "Campground"
    )

    all_button.click()

    expect(all_button).to_have_attribute("aria-pressed", "true")
    expect(nature_button).to_have_attribute("aria-pressed", "false")
    expect(
        page.locator(".location-review-candidate", has_text="Nearby General Store")
    ).to_be_visible()
    expect(campground).to_have_count(0)

    requested_types = page.evaluate(
        "() => window.__locationReviewNearbyRequests.map(request => request.type)"
    )
    assert "campground" in requested_types
    assert None in requested_types


def test_location_review_photo_marker_opens_the_photo_preview(live_server, page):
    """A photo dot opens its photo with the whole location group available."""
    photo_ids = live_server["data"]["photos"][:2]
    with live_server["db"].conn:
        live_server["db"].conn.executemany(
            "UPDATE photos SET latitude = ?, longitude = ? WHERE id = ?",
            [
                (33.2550, -116.4050, photo_ids[0]),
                (33.2554, -116.4053, photo_ids[1]),
            ],
        )
        live_server["db"].conn.execute(
            "UPDATE photos SET companion_path = ? WHERE id = ?",
            ("bird1.jpg", photo_ids[0]),
        )

    page.route("https://unpkg.com/**", _stub_leaflet)
    page.goto(f"{live_server['url']}/browse")
    page.evaluate(
        "photoIds => sessionStorage.setItem('vireoLocationReviewSource', "
        "JSON.stringify({photo_ids: photoIds}))",
        photo_ids,
    )
    page.goto(f"{live_server['url']}/locations/review?source=selection")
    expect(page.locator("#locationReviewGroupTitle")).to_have_text("2 photos")

    page.evaluate("window.__locationReviewLeafletMarkers[0].fire('click')")

    expect(page.locator("#lightboxOverlay")).to_have_class(
        "lightbox-overlay active"
    )
    expect(page.locator("#lightboxImg")).to_have_attribute(
        "src", f"/photos/{photo_ids[0]}/full"
    )
    expect(page.locator("#lightboxCounter")).to_contain_text("1 / 2")

    page.evaluate("lightboxDelete()")
    expect(page.locator("#deleteCompanionRow")).to_be_visible()
    expect(page.locator("#deleteCompanionLabel")).to_have_text(
        "Also delete 1 companion file"
    )

    with page.expect_request(
        "**/api/location-review/saved-suggestions?*"
    ) as suggestion_request:
        page.evaluate(
            """() => {
              var callback = _deleteCallback;
              hideDeleteModal();
              callback({deleted: 1});
              closeLightbox();
            }"""
        )
    suggestion_params = parse_qs(urlparse(suggestion_request.value.url).query)
    assert float(suggestion_params["lat"][0]) == pytest.approx(33.2554)
    assert float(suggestion_params["lng"][0]) == pytest.approx(-116.4053)
    expect(page.locator("#locationReviewGroupTitle")).to_have_text("1 photo")
    expect(page.locator(".location-review-thumb")).to_have_count(1)

    page.locator("#locationReviewSearch").fill("Remaining photo location")
    page.locator("#locationReviewCustom").click()
    with page.expect_request("**/api/batch/location/text") as request_info:
        page.locator("#locationReviewAssign").click()
    assignment = request_info.value.post_data_json
    assert assignment["photo_ids"] == [photo_ids[1]]
    assert assignment["latitude"] == pytest.approx(33.2554)
    assert assignment["longitude"] == pytest.approx(-116.4053)
    expect(page.locator("#locationReviewEmptyTitle")).to_have_text(
        "All locations reviewed"
    )


def test_location_review_lightbox_delete_keeps_selection_source_in_sync(
    live_server, page,
):
    """Deleting a photo through the lightbox removes it from the saved selection.

    Without this sync, reopening the review page reposts the deleted ID to
    ``/api/location-review/preview``, which returns 404 and blocks the remaining
    photos from being reviewed.
    """
    photo_ids = live_server["data"]["photos"][:2]
    with live_server["db"].conn:
        live_server["db"].conn.executemany(
            "UPDATE photos SET latitude = ?, longitude = ? WHERE id = ?",
            [
                (33.2550, -116.4050, photo_ids[0]),
                (33.2550, -116.4050, photo_ids[1]),
            ],
        )

    page.route("https://unpkg.com/**", _stub_leaflet)
    page.goto(f"{live_server['url']}/browse")
    page.evaluate(
        "photoIds => sessionStorage.setItem('vireoLocationReviewSource', "
        "JSON.stringify({photo_ids: photoIds}))",
        photo_ids,
    )
    page.goto(f"{live_server['url']}/locations/review?source=selection")
    expect(page.locator("#locationReviewGroupTitle")).to_have_text("2 photos")

    page.evaluate("window.__locationReviewLeafletMarkers[0].fire('click')")
    expect(page.locator("#lightboxOverlay")).to_have_class(
        "lightbox-overlay active"
    )

    page.evaluate("lightboxDelete()")
    page.evaluate(
        """() => {
          var callback = _deleteCallback;
          hideDeleteModal();
          callback({deleted: 1});
          closeLightbox();
        }"""
    )

    expect(page.locator("#locationReviewGroupTitle")).to_have_text("1 photo")

    stored = page.evaluate(
        "() => JSON.parse(sessionStorage.getItem('vireoLocationReviewSource'))"
    )
    assert stored == {"photo_ids": [photo_ids[1]]}
    selection_label = page.evaluate(
        """() => {
          var option = document.querySelector(
            '#locationReviewCollection option[value=\"__selection__\"]'
          );
          return option && option.textContent;
        }"""
    )
    assert selection_label == "Selected photos (1)"

    page.goto(f"{live_server['url']}/locations/review?source=selection")
    expect(page.locator("#locationReviewGroupTitle")).to_have_text("1 photo")
    expect(page.locator(".location-review-thumb")).to_have_count(1)
    expect(
        page.locator(f'.location-review-thumb[data-photo-id="{photo_ids[1]}"]')
    ).to_be_visible()


def test_location_review_thumbnail_opens_the_photo_preview(live_server, page):
    """The thumbnail strip offers the same preview affordance as map dots."""
    photo_id = live_server["data"]["photos"][0]
    with live_server["db"].conn:
        live_server["db"].conn.execute(
            "UPDATE photos SET latitude = ?, longitude = ? WHERE id = ?",
            (33.2550, -116.4050, photo_id),
        )

    page.route("https://unpkg.com/**", _stub_leaflet)
    page.goto(f"{live_server['url']}/browse")
    page.evaluate(
        "photoId => sessionStorage.setItem('vireoLocationReviewSource', "
        "JSON.stringify({photo_ids: [photoId]}))",
        photo_id,
    )
    page.goto(f"{live_server['url']}/locations/review?source=selection")

    page.locator(f'.location-review-thumb[data-photo-id="{photo_id}"]').click()

    expect(page.locator("#lightboxOverlay")).to_have_class(
        "lightbox-overlay active"
    )
    expect(page.locator("#lightboxImg")).to_have_attribute(
        "src", f"/photos/{photo_id}/full"
    )


def test_browse_review_on_map_opens_the_selected_photos(live_server, page):
    photo_id = live_server["data"]["photos"][0]
    with live_server["db"].conn:
        live_server["db"].conn.execute(
            "UPDATE photos SET latitude = ?, longitude = ? WHERE id = ?",
            (33.2550, -116.4050, photo_id),
        )

    page.route("https://unpkg.com/**", _stub_leaflet)
    page.goto(f"{live_server['url']}/browse")
    page.locator(".grid-card").first.wait_for(state="visible")
    page.evaluate(
        "photoId => { selectedPhotos.add(photoId); updateBatchBar(); }",
        photo_id,
    )

    button = page.locator("#resolveGpsSelectedBtn")
    expect(button).to_have_text("Review on Map")
    button.click()

    page.wait_for_url("**/locations/review?source=selection")
    expect(page.locator("#locationReviewGroupTitle")).to_have_text("1 photo")
    page.locator("#locationReviewSkip").click()
    expect(page.locator("#locationReviewEmptyTitle")).to_have_text(
        "Location review paused"
    )
    expect(page.locator("#locationReviewEmptyMessage")).to_contain_text(
        "skipped without changes"
    )


def test_location_review_actions_stay_above_open_bottom_panel(live_server, page):
    """Opening the shared jobs panel must not cover the review controls."""
    photo_id = live_server["data"]["photos"][0]
    with live_server["db"].conn:
        live_server["db"].conn.execute(
            "UPDATE photos SET latitude = ?, longitude = ? WHERE id = ?",
            (33.2550, -116.4050, photo_id),
        )

    page.set_viewport_size({"width": 890, "height": 600})
    page.route("https://unpkg.com/**", _stub_leaflet)
    page.goto(f"{live_server['url']}/browse")
    page.evaluate(
        "photoId => sessionStorage.setItem('vireoLocationReviewSource', "
        "JSON.stringify({photo_ids: [photoId]}))",
        photo_id,
    )
    page.goto(f"{live_server['url']}/locations/review?source=selection")
    expect(page.locator("#locationReviewGroupTitle")).to_have_text("1 photo")

    page.locator("#bpArrow").click()
    page.wait_for_function(
        "() => document.body.style.getPropertyValue('--bottom-offset') === '268px'"
    )

    positions = page.evaluate(
        """() => ({
          actionsBottom: document.querySelector('.location-review-actions')
            .getBoundingClientRect().bottom,
          panelTop: document.getElementById('bottomPanel').getBoundingClientRect().top
        })"""
    )
    assert positions["actionsBottom"] <= positions["panelTop"]
    expect(page.locator("#locationReviewAssign")).to_be_in_viewport()


def test_location_review_actions_stay_visible_below_top_banner(live_server, page):
    """Shared notification banners must resize rather than clip the review page."""
    photo_id = live_server["data"]["photos"][0]
    with live_server["db"].conn:
        live_server["db"].conn.execute(
            "UPDATE photos SET latitude = ?, longitude = ? WHERE id = ?",
            (33.2550, -116.4050, photo_id),
        )

    page.set_viewport_size({"width": 890, "height": 600})
    page.route("https://unpkg.com/**", _stub_leaflet)
    page.goto(f"{live_server['url']}/browse")
    page.evaluate(
        "photoId => sessionStorage.setItem('vireoLocationReviewSource', "
        "JSON.stringify({photo_ids: [photoId]}))",
        photo_id,
    )
    page.goto(f"{live_server['url']}/locations/review?source=selection")
    expect(page.locator("#locationReviewGroupTitle")).to_have_text("1 photo")

    page.evaluate(
        """() => {
          document.getElementById('newImagesMsg').textContent = '7 new images';
          document.getElementById('newImagesBanner').style.display = 'flex';
        }"""
    )

    positions = page.evaluate(
        """() => ({
          bannerBottom: document.getElementById('newImagesBanner')
            .getBoundingClientRect().bottom,
          reviewTop: document.querySelector('.location-review-page')
            .getBoundingClientRect().top,
          actionsBottom: document.querySelector('.location-review-actions')
            .getBoundingClientRect().bottom,
          bottomBarTop: document.getElementById('bottomToggle')
            .getBoundingClientRect().top
        })"""
    )
    assert positions["reviewTop"] >= positions["bannerBottom"]
    assert positions["actionsBottom"] <= positions["bottomBarTop"]
    expect(page.locator("#locationReviewAssign")).to_be_in_viewport()
