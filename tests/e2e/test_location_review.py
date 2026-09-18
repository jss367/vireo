import json
from pathlib import Path
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
    this.geocode = function(request, callback) {
      if (request.location.lat() >= 40) {
        function area(placeId, name, type, lat, lng) {
          return {
            place_id: placeId,
            name: name,
            types: [type, 'political'],
            formatted_address: name + ', France',
            address_components: [{long_name: name, short_name: name, types: [type, 'political']}],
            geometry: {location: new LatLng(lat, lng)}
          };
        }
        callback([
          area('garrieux', 'Garrieux', 'neighborhood', 42.810, 2.940),
          area('saint-hippolyte', 'Saint-Hippolyte', 'locality', 42.810, 2.940),
          area('pyrenees-orientales', 'Pyrénées-Orientales', 'administrative_area_level_2', 42.810, 2.940)
        ], 'OK');
        return;
      }
      if (!window.__locationReviewIncludeRegions) {
        callback([], 'ZERO_RESULTS');
        return;
      }
      callback([
        {
          place_id: 'san-diego',
          name: 'San Diego, CA, USA',
          types: ['locality'],
          address_components: [{long_name: 'San Diego', types: ['locality']}],
          formatted_address: 'San Diego, CA, USA',
          geometry: {location: request.location}
        },
        {
          place_id: 'california',
          name: 'California, USA',
          types: ['administrative_area_level_1'],
          address_components: [{long_name: 'California', types: ['administrative_area_level_1']}],
          formatted_address: 'California, USA',
          geometry: {location: request.location}
        },
        {
          place_id: 'united-states',
          name: 'United States',
          types: ['country'],
          address_components: [{long_name: 'United States', types: ['country']}],
          formatted_address: 'United States',
          geometry: {location: request.location}
        }
      ], 'OK');
    };
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


def _seed_gps_discrepancies(live_server):
    import config as cfg

    config = cfg.load()
    config['write_assigned_location_to_xmp'] = True
    cfg.save(config)
    db = live_server['db']
    photo_ids = live_server['data']['photos'][:2]
    keyword = db.get_or_create_text_location('Kumeyaay Lake')
    db.conn.execute('UPDATE keywords SET latitude=?, longitude=? WHERE id=?', (32.841515, -117.032998, keyword))
    for pid in photo_ids:
        db.set_photo_location(pid, keyword)
        db.conn.execute(
            'UPDATE photos SET latitude=?, longitude=?, timestamp=? WHERE id=?',
            (32.8360733333333, -117.029203333333, '2026-09-12T10:31:33', pid),
        )
        folder_id = db.conn.execute('SELECT folder_id FROM photos WHERE id=?', (pid,)).fetchone()[0]
        folder = Path(db._db_path).parent / f'gps-review-{folder_id}'
        folder.mkdir(exist_ok=True)
        db.conn.execute('UPDATE folders SET path=? WHERE id=?', (str(folder), folder_id))
    db.conn.commit()
    return photo_ids


def test_gps_discrepancy_review_requires_selection_and_queues_only_selected(live_server, page):
    photo_ids = _seed_gps_discrepancies(live_server)
    page_errors = []
    page.on('pageerror', lambda error: page_errors.append(str(error)))
    page.route('https://unpkg.com/**', _stub_leaflet)
    page.goto(f"{live_server['url']}/locations/review?mode=discrepancies&scope=all")
    expect(page.locator('#locationReviewGroupTitle')).to_have_text('Kumeyaay Lake')
    expect(page.locator('[data-gps-select]')).to_have_count(2)
    expect(page.locator('#locationReviewSpread')).to_contain_text('701 m')
    expect(page.locator('#locationReviewAssign')).to_be_disabled()
    expect(page.locator('#locationReviewKeep')).to_be_disabled()
    markers = page.evaluate('window.__locationReviewLeafletMarkers.map(m => m.latlng)')
    assert [32.841515, -117.032998] in markers
    assert len(markers) == 3
    page.locator(f'[data-gps-select="{photo_ids[0]}"]').check()
    page.locator('#locationReviewAssign').click()
    expect(page.locator('[data-gps-select]')).to_have_count(1)
    expect(page.locator('#locationReviewCorrectionStatus')).to_contain_text('1 correction queued')
    pending = live_server['db'].get_pending_changes()
    assert [(p['photo_id'], p['change_type']) for p in pending] == [(photo_ids[0], 'location')]
    # Keeping the second photo writes no metadata and survives reopening.
    page.locator('#locationReviewSelectAll').click()
    page.locator('#locationReviewKeep').click()
    expect(page.locator('#locationReviewEmptyTitle')).to_have_text('All locations reviewed')
    expect(page.locator('#locationReviewEmptyMessage')).to_contain_text('queued')
    page.reload()
    expect(page.locator('[data-gps-select]')).to_have_count(1)
    expect(page.locator(f'[data-gps-select="{photo_ids[0]}"]')).to_be_visible()
    # Until sync succeeds the queued correction is still reviewable.
    page.locator('#locationReviewIncludeKept').check()
    expect(page.locator('[data-gps-select]')).to_have_count(2)
    page.get_by_role('button', name='Review queued metadata changes', exact=True).first.click()
    expect(page.locator('#syncPreviewOverlay')).to_be_visible()
    expect(page.locator('#syncPreviewContent')).to_contain_text('Kumeyaay Lake')
    assert page_errors == []


def test_gps_discrepancy_skip_and_distance_filter_do_not_change_metadata(live_server, page):
    _seed_gps_discrepancies(live_server)
    page.route('https://unpkg.com/**', _stub_leaflet)
    page.goto(f"{live_server['url']}/locations/review?mode=discrepancies&scope=all")
    expect(page.locator('#locationReviewGroupTitle')).to_have_text('Kumeyaay Lake')
    page.locator('#locationReviewSkip').click()
    expect(page.locator('#locationReviewEmptyTitle')).to_have_text('Location review paused')
    page.reload()
    expect(page.locator('[data-gps-select]')).to_have_count(2)
    page.locator('#locationReviewDistance').fill('1000')
    page.locator('#locationReviewDistance').press('Tab')
    expect(page.locator('#locationReviewEmptyTitle')).to_have_text('No locations to review')
    assert live_server['db'].get_pending_changes() == []
    assert live_server['db'].conn.execute('SELECT COUNT(*) FROM location_gps_reviews').fetchone()[0] == 0


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
        "San Diego Field Notes (1 available)"
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

    success_toast = page.locator("#toastContainer > div").last
    expect(success_toast).to_have_text(
        'Assigned “Anza-Borrego Desert State Park” to 2 photos'
    )
    assert success_toast.evaluate("el => el.style.background") == "var(--success)"
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


def test_location_review_reports_and_resumes_batch_assignment_progress(
    live_server, page,
):
    """Large groups report committed chunks and retry only the remainder."""
    photo_id = live_server["data"]["photos"][0]
    synthetic_photo_ids = list(range(1, 2502))
    preview = {
        "total": len(synthetic_photo_ids),
        "reviewable": len(synthetic_photo_ids),
        "unresolved": [],
        "skipped": [],
        "groups": [{
            "photo_ids": synthetic_photo_ids,
            "photos": [{
                "id": photo_id,
                "filename": "batch-example.jpg",
                "latitude": 33.255,
                "longitude": -116.405,
                "timestamp": "2026-08-04T10:17:00",
            }],
            "count": len(synthetic_photo_ids),
            "center": {"lat": 33.255, "lng": -116.405},
            "bounds": {
                "south": 33.255,
                "west": -116.405,
                "north": 33.255,
                "east": -116.405,
            },
            "spread_m": 0,
            "captured_from": "2026-08-04T10:17:00",
            "captured_to": "2026-08-04T10:17:00",
        }],
    }

    page.route("https://unpkg.com/**", _stub_leaflet)
    page.goto(f"{live_server['url']}/browse")
    page.evaluate(
        "photoId => sessionStorage.setItem('vireoLocationReviewSource', "
        "JSON.stringify({photo_ids: [photoId]}))",
        photo_id,
    )
    page.route(
        "**/api/location-review/preview",
        lambda route: route.fulfill(json=preview),
    )
    page.goto(f"{live_server['url']}/locations/review?source=selection")
    expect(page.locator("#locationReviewGroupTitle")).to_have_text(
        "2,501 photos"
    )

    page.locator("#locationReviewSearch").fill("Large batch location")
    page.locator("#locationReviewCustom").click()
    page.evaluate(
        """() => {
          window.__locationReviewOriginalSafeFetch = window.safeFetch;
          window.__locationReviewAssignmentRequests = [];
          window.__locationReviewAssignmentPending = [];
          window.safeFetch = function(url, opts, options) {
            if (url !== '/api/batch/location/text') {
              return window.__locationReviewOriginalSafeFetch(url, opts, options);
            }
            window.__locationReviewAssignmentRequests.push(JSON.parse(opts.body));
            return new Promise(function(resolve, reject) {
              window.__locationReviewAssignmentPending.push({resolve: resolve, reject: reject});
            });
          };
          window.__settleLocationAssignment = function(result) {
            var pending = window.__locationReviewAssignmentPending.shift();
            if (result === 'reject') pending.reject(new Error('Synthetic interruption'));
            else pending.resolve({ok: true, updated: result});
          };
        }"""
    )

    page.locator("#locationReviewAssign").click()
    page.wait_for_function(
        "() => window.__locationReviewAssignmentRequests.length === 1"
    )
    progress = page.locator("#locationReviewAssignmentProgress")
    progress_bar = page.locator("#locationReviewAssignmentTrack")
    expect(progress).to_be_visible()
    expect(page.locator("#locationReviewAssignmentStatus")).to_have_text(
        "Assigning 0 of 2,501 photos · 0%"
    )
    expect(progress_bar).to_have_attribute("aria-valuemax", "2501")
    expect(progress_bar).to_have_attribute("aria-valuenow", "0")
    expect(page.locator("#locationReviewSkip")).to_be_disabled()

    page.evaluate("window.__settleLocationAssignment(1000)")
    page.wait_for_function(
        "() => window.__locationReviewAssignmentRequests.length === 2"
    )
    expect(page.locator("#locationReviewAssignmentStatus")).to_have_text(
        "Assigning 1,000 of 2,501 photos · 40%"
    )
    expect(progress_bar).to_have_attribute("aria-valuenow", "1000")

    page.evaluate("window.__settleLocationAssignment('reject')")
    expect(page.locator("#locationReviewAssign")).to_have_text(
        "Retry 1,501 remaining"
    )
    expect(page.locator("#locationReviewAssignmentStatus")).to_have_text(
        "1,000 of 2,501 assigned · 1,501 remaining"
    )
    # Skip / Prev / Next stay locked while a partial assignment is pending so
    # the committed chunks cannot be silently dropped: skipping would splice
    # the group as "skipped without changes" and navigating away would clear
    # state.assignment so a later retry reprocesses committed chunks.
    expect(page.locator("#locationReviewSkip")).to_be_disabled()

    page.locator("#locationReviewAssign").click()
    page.wait_for_function(
        "() => window.__locationReviewAssignmentRequests.length === 3"
    )
    page.evaluate("window.__settleLocationAssignment(1000)")
    page.wait_for_function(
        "() => window.__locationReviewAssignmentRequests.length === 4"
    )
    expect(page.locator("#locationReviewAssignmentStatus")).to_have_text(
        "Assigning 2,000 of 2,501 photos · 80%"
    )
    page.evaluate("window.__settleLocationAssignment(501)")

    expect(page.locator("#locationReviewEmptyTitle")).to_have_text(
        "All locations reviewed"
    )
    requests = page.evaluate("window.__locationReviewAssignmentRequests")
    assert [len(request["photo_ids"]) for request in requests] == [
        1000, 1000, 1000, 501,
    ]
    assert requests[2]["photo_ids"] == synthetic_photo_ids[1000:2000]


def test_location_review_locks_suggestion_mode_controls_during_assignment(
    live_server, page,
):
    """Suggestion-mode buttons must not clear the selection mid-assignment."""
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
    expect(page.locator("#locationReviewGroupTitle")).to_have_text("1 photo")

    recommended_button = page.locator('[data-suggestion-mode="recommended"]')
    places_button = page.locator('[data-suggestion-mode="places"]')
    expect(recommended_button).to_be_enabled()
    expect(places_button).to_be_enabled()

    page.locator("#locationReviewSearch").fill("Assigned custom place")
    page.locator("#locationReviewCustom").click()
    expect(page.locator("#locationReviewAssign")).to_contain_text(
        "Assign “Assigned custom place”"
    )

    page.evaluate(
        """() => {
          window.__locationReviewOriginalSafeFetch = window.safeFetch;
          window.__locationReviewAssignmentPending = [];
          window.safeFetch = function(url, opts, options) {
            if (url !== '/api/batch/location/text') {
              return window.__locationReviewOriginalSafeFetch(url, opts, options);
            }
            return new Promise(function(resolve, reject) {
              window.__locationReviewAssignmentPending.push(
                {resolve: resolve, reject: reject}
              );
            });
          };
          window.__settleLocationAssignment = function(result) {
            var pending = window.__locationReviewAssignmentPending.shift();
            if (result === 'reject') pending.reject(new Error('Synthetic interruption'));
            else pending.resolve({ok: true, updated: result});
          };
        }"""
    )

    page.locator("#locationReviewAssign").click()
    page.wait_for_function(
        "() => window.__locationReviewAssignmentPending.length === 1"
    )
    expect(recommended_button).to_be_disabled()
    expect(places_button).to_be_disabled()

    places_button.click(force=True)
    expect(recommended_button).to_have_attribute("aria-pressed", "true")
    expect(places_button).to_have_attribute("aria-pressed", "false")
    expect(page.locator("#locationReviewAssign")).to_have_text("Assigning…")

    page.evaluate("window.__settleLocationAssignment('reject')")
    expect(page.locator("#locationReviewAssign")).to_contain_text(
        "Retry “Assigned custom place”"
    )
    expect(recommended_button).to_be_enabled()
    expect(places_button).to_be_enabled()


def test_location_review_partial_assignment_locks_navigation_and_choice(
    live_server, page,
):
    """After a chunk failure with committed chunks, nav and choice-change are locked."""
    photo_id = live_server["data"]["photos"][0]
    other_photo_ids = list(range(2000, 2010))
    preview = {
        "total": len(other_photo_ids) + 1,
        "reviewable": len(other_photo_ids) + 1,
        "unresolved": [],
        "skipped": [],
        "groups": [
            {
                "photo_ids": list(range(1, 1501)),
                "photos": [{
                    "id": photo_id,
                    "filename": "batch-example.jpg",
                    "latitude": 33.255,
                    "longitude": -116.405,
                    "timestamp": "2026-08-04T10:17:00",
                }],
                "count": 1500,
                "center": {"lat": 33.255, "lng": -116.405},
                "bounds": {
                    "south": 33.255, "west": -116.405,
                    "north": 33.255, "east": -116.405,
                },
                "spread_m": 0,
                "captured_from": "2026-08-04T10:17:00",
                "captured_to": "2026-08-04T10:17:00",
            },
            {
                "photo_ids": other_photo_ids,
                "photos": [{
                    "id": photo_id,
                    "filename": "other-group.jpg",
                    "latitude": 40.0,
                    "longitude": -105.0,
                    "timestamp": "2026-08-04T10:20:00",
                }],
                "count": len(other_photo_ids),
                "center": {"lat": 40.0, "lng": -105.0},
                "bounds": {
                    "south": 40.0, "west": -105.0,
                    "north": 40.0, "east": -105.0,
                },
                "spread_m": 0,
                "captured_from": "2026-08-04T10:20:00",
                "captured_to": "2026-08-04T10:20:00",
            },
        ],
    }

    page.route("https://unpkg.com/**", _stub_leaflet)
    page.goto(f"{live_server['url']}/browse")
    page.evaluate(
        "photoId => sessionStorage.setItem('vireoLocationReviewSource', "
        "JSON.stringify({photo_ids: [photoId]}))",
        photo_id,
    )
    page.route(
        "**/api/location-review/preview",
        lambda route: route.fulfill(json=preview),
    )
    page.goto(f"{live_server['url']}/locations/review?source=selection")
    expect(page.locator("#locationReviewGroupTitle")).to_have_text("1,500 photos")

    # First, assign a keyword-like custom choice so we have something selected.
    page.locator("#locationReviewSearch").fill("First location")
    page.locator("#locationReviewCustom").click()
    page.evaluate(
        """() => {
          window.__originalSafeFetch = window.safeFetch;
          window.__pending = [];
          window.safeFetch = function(url, opts, options) {
            if (url !== '/api/batch/location/text') {
              return window.__originalSafeFetch(url, opts, options);
            }
            return new Promise(function(resolve, reject) {
              window.__pending.push({resolve: resolve, reject: reject});
            });
          };
          window.__settle = function(result) {
            var pending = window.__pending.shift();
            if (result === 'reject') pending.reject(new Error('boom'));
            else pending.resolve({ok: true, updated: result});
          };
        }"""
    )

    page.locator("#locationReviewAssign").click()
    page.wait_for_function("() => window.__pending.length === 1")
    page.evaluate("window.__settle(1000)")  # First chunk commits.
    page.wait_for_function("() => window.__pending.length === 1")
    page.evaluate("window.__settle('reject')")  # Second chunk fails.

    # Partial progress: nav & suggestion-mode locked until retry completes.
    expect(page.locator("#locationReviewAssign")).to_contain_text("Retry 500 remaining")
    expect(page.locator("#locationReviewSkip")).to_be_disabled()
    expect(page.locator("#locationReviewNext")).to_be_disabled()
    expect(page.locator("#locationReviewPrevious")).to_be_disabled()
    expect(page.locator('[data-suggestion-mode="nature"]')).to_be_disabled()
    expect(page.locator('[data-suggestion-mode="places"]')).to_be_disabled()

    # Attempting to select a different candidate is refused so the committed
    # chunks are not silently orphaned. The custom-name button funnels through
    # selectChoice(), so it's the easiest UI path to trigger a choice change.
    page.evaluate(
        """() => {
          window.__toasts = [];
          var original = window.showToast;
          window.showToast = function(message, kind) {
            window.__toasts.push({message: message, kind: kind});
            return original ? original(message, kind) : null;
          };
        }"""
    )
    page.locator("#locationReviewSearch").fill("Different location")
    page.locator("#locationReviewCustom").click()
    toasts = page.evaluate("window.__toasts")
    assert toasts, "expected a warning toast when changing choice mid-partial"
    assert "First location" in toasts[0]["message"]
    # The selected choice must be unchanged (button text still describes the
    # in-flight retry for the original choice).
    expect(page.locator("#locationReviewAssign")).to_contain_text("Retry 500 remaining")

    # Retrying completes the group and unlocks navigation for the next one.
    page.locator("#locationReviewAssign").click()
    page.wait_for_function("() => window.__pending.length === 1")
    page.evaluate("window.__settle(500)")
    expect(page.locator("#locationReviewGroupTitle")).to_have_text("10 photos")
    expect(page.locator("#locationReviewSkip")).to_be_enabled()


def test_location_review_partial_assignment_blocks_photo_preview(
    live_server, page,
):
    """Lightbox open is refused mid-partial so a delete can't drop committed chunks.

    Without this guard the lightbox 'photodeleted' handler would call
    renderCurrentGroup(), nulling state.assignment and unlocking navigation
    even though earlier chunks already changed photos on disk.
    """
    photo_id = live_server["data"]["photos"][0]
    preview = {
        "total": 1500,
        "reviewable": 1500,
        "unresolved": [],
        "skipped": [],
        "groups": [
            {
                "photo_ids": list(range(1, 1501)),
                "photos": [{
                    "id": photo_id,
                    "filename": "batch-example.jpg",
                    "latitude": 33.255,
                    "longitude": -116.405,
                    "timestamp": "2026-08-04T10:17:00",
                }],
                "count": 1500,
                "center": {"lat": 33.255, "lng": -116.405},
                "bounds": {
                    "south": 33.255, "west": -116.405,
                    "north": 33.255, "east": -116.405,
                },
                "spread_m": 0,
                "captured_from": "2026-08-04T10:17:00",
                "captured_to": "2026-08-04T10:17:00",
            },
        ],
    }

    page.route("https://unpkg.com/**", _stub_leaflet)
    page.goto(f"{live_server['url']}/browse")
    page.evaluate(
        "photoId => sessionStorage.setItem('vireoLocationReviewSource', "
        "JSON.stringify({photo_ids: [photoId]}))",
        photo_id,
    )
    page.route(
        "**/api/location-review/preview",
        lambda route: route.fulfill(json=preview),
    )
    page.goto(f"{live_server['url']}/locations/review?source=selection")
    expect(page.locator("#locationReviewGroupTitle")).to_have_text("1,500 photos")

    page.locator("#locationReviewSearch").fill("Partial location")
    page.locator("#locationReviewCustom").click()
    page.evaluate(
        """() => {
          window.__originalSafeFetch = window.safeFetch;
          window.__pending = [];
          window.safeFetch = function(url, opts, options) {
            if (url !== '/api/batch/location/text') {
              return window.__originalSafeFetch(url, opts, options);
            }
            return new Promise(function(resolve, reject) {
              window.__pending.push({resolve: resolve, reject: reject});
            });
          };
          window.__settle = function(result) {
            var pending = window.__pending.shift();
            if (result === 'reject') pending.reject(new Error('boom'));
            else pending.resolve({ok: true, updated: result});
          };
          window.__toasts = [];
          var originalToast = window.showToast;
          window.showToast = function(message, kind) {
            window.__toasts.push({message: message, kind: kind});
            return originalToast ? originalToast(message, kind) : null;
          };
        }"""
    )

    page.locator("#locationReviewAssign").click()
    page.wait_for_function("() => window.__pending.length === 1")
    page.evaluate("window.__settle(1000)")  # First chunk commits.
    page.wait_for_function("() => window.__pending.length === 1")
    page.evaluate("window.__settle('reject')")  # Second chunk fails.

    expect(page.locator("#locationReviewAssign")).to_contain_text("Retry 500 remaining")

    # Attempting to open a photo via the thumbnail must be refused while a
    # partial assignment is pending. Otherwise 'lightbox:photodeleted' would
    # call renderCurrentGroup() and silently orphan the committed chunks.
    page.locator(
        f'.location-review-thumb[data-photo-id="{photo_id}"]'
    ).click()

    expect(page.locator("#lightboxOverlay")).not_to_have_class(
        "lightbox-overlay active"
    )
    toasts = page.evaluate("window.__toasts")
    assert toasts, "expected a warning toast when opening a photo mid-partial"
    assert "pending assignment" in toasts[-1]["message"]
    assert "Partial location" in toasts[-1]["message"]

    # Partial-assignment state must survive the blocked click intact — the
    # retry button still points at the in-flight remainder and every nav
    # control stays locked.
    expect(page.locator("#locationReviewAssign")).to_contain_text("Retry 500 remaining")
    expect(page.locator("#locationReviewSkip")).to_be_disabled()
    expect(page.locator("#locationReviewNext")).to_be_disabled()
    expect(page.locator("#locationReviewPrevious")).to_be_disabled()

    # Retrying completes the group; only then should the lightbox open again.
    page.locator("#locationReviewAssign").click()
    page.wait_for_function("() => window.__pending.length === 1")
    page.evaluate("window.__settle(500)")
    expect(page.locator("#locationReviewEmptyTitle")).to_have_text(
        "All locations reviewed"
    )


def test_location_review_assignment_closes_lightbox_already_open(
    live_server, page,
):
    """A lightbox left open before assignment starts is closed by assignCurrentGroup.

    openPhotoPreview()'s guard only blocks NEW opens once state.isAssigning or
    hasPartialAssignmentProgress becomes true; a lightbox opened moments before
    (e.g. a keyboard user tabs from a thumbnail to Assign) survives past those
    checks. If it survives further, deleting the visible photo mid-assignment
    would fire lightbox:photodeleted → renderCurrentGroup(), nulling
    state.assignment while committed chunks are still in flight.
    """
    photo_id = live_server["data"]["photos"][0]
    preview = {
        "total": 1500,
        "reviewable": 1500,
        "unresolved": [],
        "skipped": [],
        "groups": [
            {
                "photo_ids": list(range(1, 1501)),
                "photos": [{
                    "id": photo_id,
                    "filename": "batch-example.jpg",
                    "latitude": 33.255,
                    "longitude": -116.405,
                    "timestamp": "2026-08-04T10:17:00",
                }],
                "count": 1500,
                "center": {"lat": 33.255, "lng": -116.405},
                "bounds": {
                    "south": 33.255, "west": -116.405,
                    "north": 33.255, "east": -116.405,
                },
                "spread_m": 0,
                "captured_from": "2026-08-04T10:17:00",
                "captured_to": "2026-08-04T10:17:00",
            },
        ],
    }

    page.route("https://unpkg.com/**", _stub_leaflet)
    page.goto(f"{live_server['url']}/browse")
    page.evaluate(
        "photoId => sessionStorage.setItem('vireoLocationReviewSource', "
        "JSON.stringify({photo_ids: [photoId]}))",
        photo_id,
    )
    page.route(
        "**/api/location-review/preview",
        lambda route: route.fulfill(json=preview),
    )
    page.goto(f"{live_server['url']}/locations/review?source=selection")
    expect(page.locator("#locationReviewGroupTitle")).to_have_text("1,500 photos")

    page.locator("#locationReviewSearch").fill("Pre-open lightbox")
    page.locator("#locationReviewCustom").click()

    page.evaluate(
        """() => {
          window.__originalSafeFetch = window.safeFetch;
          window.__pending = [];
          window.safeFetch = function(url, opts, options) {
            if (url !== '/api/batch/location/text') {
              return window.__originalSafeFetch(url, opts, options);
            }
            return new Promise(function(resolve, reject) {
              window.__pending.push({resolve: resolve, reject: reject});
            });
          };
          window.__settle = function(result) {
            var pending = window.__pending.shift();
            if (result === 'reject') pending.reject(new Error('boom'));
            else pending.resolve({ok: true, updated: result});
          };
        }"""
    )

    # Open the lightbox BEFORE the assignment starts — the openPhotoPreview
    # guard permits this because nothing is in flight yet. Drive openLightbox
    # directly so this test doesn't depend on the browse-page thumbnail
    # loading a real photo bitmap.
    page.evaluate(
        """photoId => {
          openLightbox(photoId, 'batch-example.jpg', [
            {id: photoId, filename: 'batch-example.jpg'}
          ]);
        }""",
        photo_id,
    )
    expect(page.locator("#lightboxOverlay")).to_have_class(
        "lightbox-overlay active"
    )

    # Simulate the keyboard-tab-to-Assign path: dispatch the click via the DOM
    # so it reaches the button beneath the overlay. Codex's concern is that
    # the Assign handler is reachable at all while the lightbox is up, not
    # the specific interaction that gets there.
    page.evaluate(
        "document.getElementById('locationReviewAssign').click()"
    )
    page.wait_for_function("() => window.__pending.length === 1")

    # assignCurrentGroup must have closed the lightbox before the first chunk
    # fired — otherwise a delete would still be able to reset state.assignment.
    expect(page.locator("#lightboxOverlay")).not_to_have_class(
        "lightbox-overlay active"
    )
    assert page.evaluate("window._lbEscToken == null")

    page.evaluate("window.__settle(1000)")
    page.wait_for_function("() => window.__pending.length === 1")
    page.evaluate("window.__settle(500)")
    expect(page.locator("#locationReviewEmptyTitle")).to_have_text(
        "All locations reviewed"
    )


def test_location_review_partial_assignment_survives_photodeleted_event(
    live_server, page,
):
    """The lightbox:photodeleted handler preserves state.assignment mid-partial.

    Defense in depth for the primary close-on-assignment fix: even if a
    photodeleted event somehow fires while a partial batch is waiting for
    retry, the handler must NOT rebuild the group and null the recorded
    offset — that would silently reprocess already-committed chunks on retry.
    """
    photo_id = live_server["data"]["photos"][0]
    photo_ids = list(range(1, 1501))
    preview = {
        "total": 1500,
        "reviewable": 1500,
        "unresolved": [],
        "skipped": [],
        "groups": [
            {
                "photo_ids": photo_ids,
                "photos": [
                    {
                        "id": photo_id,
                        "filename": "keep.jpg",
                        "latitude": 33.255,
                        "longitude": -116.405,
                        "timestamp": "2026-08-04T10:17:00",
                    },
                    {
                        "id": photo_id + 9001,
                        "filename": "drop.jpg",
                        "latitude": 33.255,
                        "longitude": -116.405,
                        "timestamp": "2026-08-04T10:18:00",
                    },
                ],
                "count": 1500,
                "center": {"lat": 33.255, "lng": -116.405},
                "bounds": {
                    "south": 33.255, "west": -116.405,
                    "north": 33.255, "east": -116.405,
                },
                "spread_m": 0,
                "captured_from": "2026-08-04T10:17:00",
                "captured_to": "2026-08-04T10:18:00",
            },
        ],
    }

    page.route("https://unpkg.com/**", _stub_leaflet)
    page.goto(f"{live_server['url']}/browse")
    page.evaluate(
        "photoId => sessionStorage.setItem('vireoLocationReviewSource', "
        "JSON.stringify({photo_ids: [photoId]}))",
        photo_id,
    )
    page.route(
        "**/api/location-review/preview",
        lambda route: route.fulfill(json=preview),
    )
    page.goto(f"{live_server['url']}/locations/review?source=selection")
    expect(page.locator("#locationReviewGroupTitle")).to_have_text("1,500 photos")

    page.locator("#locationReviewSearch").fill("Defensive location")
    page.locator("#locationReviewCustom").click()
    page.evaluate(
        """() => {
          window.__originalSafeFetch = window.safeFetch;
          window.__pending = [];
          window.safeFetch = function(url, opts, options) {
            if (url !== '/api/batch/location/text') {
              return window.__originalSafeFetch(url, opts, options);
            }
            return new Promise(function(resolve, reject) {
              window.__pending.push({resolve: resolve, reject: reject});
            });
          };
          window.__settle = function(result) {
            var pending = window.__pending.shift();
            if (result === 'reject') pending.reject(new Error('boom'));
            else pending.resolve({ok: true, updated: result});
          };
        }"""
    )

    page.evaluate(
        """() => {
          window.__requestBodies = [];
          var raw = window.safeFetch;
          window.safeFetch = function(url, opts, options) {
            if (url === '/api/batch/location/text') {
              window.__requestBodies.push(JSON.parse(opts.body));
            }
            return raw(url, opts, options);
          };
        }"""
    )

    page.locator("#locationReviewAssign").click()
    page.wait_for_function("() => window.__pending.length === 1")
    page.evaluate("window.__settle(1000)")  # first chunk commits
    page.wait_for_function("() => window.__pending.length === 1")
    page.evaluate("window.__settle('reject')")  # second chunk fails

    expect(page.locator("#locationReviewAssign")).to_contain_text(
        "Retry 500 remaining"
    )
    assert page.evaluate("window.__requestBodies.length") == 2

    # Synthesize a stray photodeleted event for a photo in the current group.
    page.evaluate(
        "photoId => document.dispatchEvent(new CustomEvent('lightbox:photodeleted',"
        " {detail: {photoId: photoId + 9001}}))",
        photo_id,
    )

    # Nav and choice-mode buttons must stay locked — the observable proof that
    # the handler did NOT rebuild the UI and null state.assignment.
    expect(page.locator("#locationReviewAssign")).to_contain_text(
        "Retry 500 remaining"
    )
    expect(page.locator("#locationReviewSkip")).to_be_disabled()
    expect(page.locator("#locationReviewNext")).to_be_disabled()
    expect(page.locator("#locationReviewPrevious")).to_be_disabled()
    expect(page.locator('[data-suggestion-mode="nature"]')).to_be_disabled()
    expect(page.locator('[data-suggestion-mode="places"]')).to_be_disabled()

    # Retry completes at the correct offset — no reprocessing of committed
    # chunks. If the handler had reset state.assignment, retry would start at
    # offset 0 and send 1500 photos again across two more chunks.
    page.locator("#locationReviewAssign").click()
    page.wait_for_function("() => window.__pending.length === 1")
    page.evaluate("window.__settle(500)")
    expect(page.locator("#locationReviewEmptyTitle")).to_have_text(
        "All locations reviewed"
    )
    request_lengths = page.evaluate(
        "window.__requestBodies.map(function(body) { return body.photo_ids.length; })"
    )
    assert request_lengths == [1000, 500, 500], (
        f"Retry should resume from offset 1000, got chunks {request_lengths}"
    )
    third_chunk_ids = page.evaluate("window.__requestBodies[2].photo_ids")
    assert third_chunk_ids == list(range(1001, 1501))


def test_location_review_partial_assignment_survives_completed_prefix_delete(
    live_server, page,
):
    """A photodeleted event for an already-committed photo mustn't shift the retry offset.

    If lightbox:photodeleted removes a photo from the already-committed
    prefix of group.photo_ids, the surviving IDs slide left. If the retry
    loop slices group.photo_ids at the stale offset, the first
    still-unassigned photo would be skipped and the group silently marked
    completed. Assignment must slice from an immutable snapshot instead.
    """
    photo_id = live_server["data"]["photos"][0]
    photo_ids = list(range(1, 1501))
    preview = {
        "total": 1500,
        "reviewable": 1500,
        "unresolved": [],
        "skipped": [],
        "groups": [
            {
                "photo_ids": photo_ids,
                "photos": [
                    {
                        "id": photo_id,
                        "filename": "keep.jpg",
                        "latitude": 33.255,
                        "longitude": -116.405,
                        "timestamp": "2026-08-04T10:17:00",
                    },
                ],
                "count": 1500,
                "center": {"lat": 33.255, "lng": -116.405},
                "bounds": {
                    "south": 33.255, "west": -116.405,
                    "north": 33.255, "east": -116.405,
                },
                "spread_m": 0,
                "captured_from": "2026-08-04T10:17:00",
                "captured_to": "2026-08-04T10:17:00",
            },
        ],
    }

    page.route("https://unpkg.com/**", _stub_leaflet)
    page.goto(f"{live_server['url']}/browse")
    page.evaluate(
        "photoId => sessionStorage.setItem('vireoLocationReviewSource', "
        "JSON.stringify({photo_ids: [photoId]}))",
        photo_id,
    )
    page.route(
        "**/api/location-review/preview",
        lambda route: route.fulfill(json=preview),
    )
    page.goto(f"{live_server['url']}/locations/review?source=selection")
    expect(page.locator("#locationReviewGroupTitle")).to_have_text("1,500 photos")

    page.locator("#locationReviewSearch").fill("Prefix delete location")
    page.locator("#locationReviewCustom").click()
    page.evaluate(
        """() => {
          window.__originalSafeFetch = window.safeFetch;
          window.__pending = [];
          window.__requestBodies = [];
          window.safeFetch = function(url, opts, options) {
            if (url !== '/api/batch/location/text') {
              return window.__originalSafeFetch(url, opts, options);
            }
            window.__requestBodies.push(JSON.parse(opts.body));
            return new Promise(function(resolve, reject) {
              window.__pending.push({resolve: resolve, reject: reject});
            });
          };
          window.__settle = function(result) {
            var pending = window.__pending.shift();
            if (result === 'reject') pending.reject(new Error('boom'));
            else pending.resolve({ok: true, updated: result});
          };
        }"""
    )

    page.locator("#locationReviewAssign").click()
    page.wait_for_function("() => window.__pending.length === 1")
    page.evaluate("window.__settle(1000)")  # first chunk commits
    page.wait_for_function("() => window.__pending.length === 1")
    page.evaluate("window.__settle('reject')")  # second chunk fails

    expect(page.locator("#locationReviewAssign")).to_contain_text(
        "Retry 500 remaining"
    )
    assert page.evaluate("window.__requestBodies.length") == 2

    # Simulate a photodeleted event for a photo in the already-committed
    # prefix (ID 500 was in the first chunk, indices 0..999). Before the
    # snapshot fix, the delete handler's filter left group.photo_ids with
    # 1499 elements and shifted IDs 501..1500 down to indices 500..1498;
    # retry would then slice from offset 1000, sending IDs 1002..1500 and
    # silently skipping ID 1001.
    page.evaluate(
        "document.dispatchEvent(new CustomEvent('lightbox:photodeleted',"
        " {detail: {photoId: 500}}))"
    )

    # Navigation must stay locked — partial state is still pending.
    expect(page.locator("#locationReviewSkip")).to_be_disabled()
    expect(page.locator("#locationReviewNext")).to_be_disabled()
    expect(page.locator("#locationReviewPrevious")).to_be_disabled()

    page.locator("#locationReviewAssign").click()
    page.wait_for_function("() => window.__pending.length === 1")
    page.evaluate("window.__settle(500)")
    expect(page.locator("#locationReviewEmptyTitle")).to_have_text(
        "All locations reviewed"
    )
    request_lengths = page.evaluate(
        "window.__requestBodies.map(function(body) { return body.photo_ids.length; })"
    )
    assert request_lengths == [1000, 500, 500], (
        f"Retry should resume from offset 1000 with 500 IDs, got chunks {request_lengths}"
    )
    third_chunk_ids = page.evaluate("window.__requestBodies[2].photo_ids")
    assert third_chunk_ids == list(range(1001, 1501)), (
        "Retry chunk must contain the original pending IDs (1001..1500) — "
        "the completed-prefix delete must not shift the snapshot."
    )


def test_location_review_partial_assignment_drops_deleted_pending_ids(
    live_server, page,
):
    """Deleting a photo in the retry suffix must not leave its ID in the snapshot.

    Both batch-location endpoints validate every photo_id up front and 404
    on any that no longer exists. If a lightbox:photodeleted event removes
    a photo from the still-unassigned suffix while a partial batch waits
    for retry, the immutable snapshot would keep that ID and every retry
    would 404 — permanently jamming the group. The reconciled snapshot
    must drop the deleted ID and its progress counters.
    """
    photo_id = live_server["data"]["photos"][0]
    photo_ids = list(range(1, 1501))
    preview = {
        "total": 1500,
        "reviewable": 1500,
        "unresolved": [],
        "skipped": [],
        "groups": [
            {
                "photo_ids": photo_ids,
                "photos": [
                    {
                        "id": photo_id,
                        "filename": "keep.jpg",
                        "latitude": 33.255,
                        "longitude": -116.405,
                        "timestamp": "2026-08-04T10:17:00",
                    },
                ],
                "count": 1500,
                "center": {"lat": 33.255, "lng": -116.405},
                "bounds": {
                    "south": 33.255, "west": -116.405,
                    "north": 33.255, "east": -116.405,
                },
                "spread_m": 0,
                "captured_from": "2026-08-04T10:17:00",
                "captured_to": "2026-08-04T10:17:00",
            },
        ],
    }

    page.route("https://unpkg.com/**", _stub_leaflet)
    page.goto(f"{live_server['url']}/browse")
    page.evaluate(
        "photoId => sessionStorage.setItem('vireoLocationReviewSource', "
        "JSON.stringify({photo_ids: [photoId]}))",
        photo_id,
    )
    page.route(
        "**/api/location-review/preview",
        lambda route: route.fulfill(json=preview),
    )
    page.goto(f"{live_server['url']}/locations/review?source=selection")
    expect(page.locator("#locationReviewGroupTitle")).to_have_text("1,500 photos")

    page.locator("#locationReviewSearch").fill("Suffix delete location")
    page.locator("#locationReviewCustom").click()
    page.evaluate(
        """() => {
          window.__originalSafeFetch = window.safeFetch;
          window.__pending = [];
          window.__requestBodies = [];
          window.safeFetch = function(url, opts, options) {
            if (url !== '/api/batch/location/text') {
              return window.__originalSafeFetch(url, opts, options);
            }
            window.__requestBodies.push(JSON.parse(opts.body));
            return new Promise(function(resolve, reject) {
              window.__pending.push({resolve: resolve, reject: reject});
            });
          };
          window.__settle = function(result) {
            var pending = window.__pending.shift();
            if (result === 'reject') pending.reject(new Error('boom'));
            else pending.resolve({ok: true, updated: result});
          };
        }"""
    )

    page.locator("#locationReviewAssign").click()
    page.wait_for_function("() => window.__pending.length === 1")
    page.evaluate("window.__settle(1000)")  # first chunk commits
    page.wait_for_function("() => window.__pending.length === 1")
    page.evaluate("window.__settle('reject')")  # second chunk fails

    expect(page.locator("#locationReviewAssign")).to_contain_text(
        "Retry 500 remaining"
    )
    assert page.evaluate("window.__requestBodies.length") == 2

    # Simulate a photodeleted event for a photo in the still-unassigned
    # suffix (ID 1200 was in the pending chunk 1001..1500). Without the
    # snapshot reconciliation, the retry snapshot would still contain
    # 1200 and every retry would 404 on the missing photo, leaving the
    # user stuck behind a locked toolbar.
    page.evaluate(
        "document.dispatchEvent(new CustomEvent('lightbox:photodeleted',"
        " {detail: {photoId: 1200}}))"
    )

    # Progress and retry button reflect the reconciled remainder (500 - 1).
    expect(page.locator("#locationReviewAssign")).to_contain_text(
        "Retry 499 remaining"
    )
    expect(page.locator("#locationReviewAssignmentStatus")).to_have_text(
        "1,000 of 1,499 assigned · 499 remaining"
    )
    # Navigation must stay locked — partial state is still pending.
    expect(page.locator("#locationReviewSkip")).to_be_disabled()
    expect(page.locator("#locationReviewNext")).to_be_disabled()
    expect(page.locator("#locationReviewPrevious")).to_be_disabled()

    page.locator("#locationReviewAssign").click()
    page.wait_for_function("() => window.__pending.length === 1")
    page.evaluate("window.__settle(499)")
    expect(page.locator("#locationReviewEmptyTitle")).to_have_text(
        "All locations reviewed"
    )
    request_lengths = page.evaluate(
        "window.__requestBodies.map(function(body) { return body.photo_ids.length; })"
    )
    assert request_lengths == [1000, 500, 499], (
        f"Retry must skip the deleted pending ID, got chunks {request_lengths}"
    )
    third_chunk_ids = page.evaluate("window.__requestBodies[2].photo_ids")
    expected_third = [pid for pid in range(1001, 1501) if pid != 1200]
    assert third_chunk_ids == expected_third, (
        "Retry chunk must exclude the deleted pending ID — the snapshot must "
        "reconcile with lightbox:photodeleted."
    )


def test_location_review_partial_assignment_advances_when_deletion_drains_suffix(
    live_server, page,
):
    """A deletion that empties the retry suffix must advance the group.

    When a partial batch fails and the remaining unassigned photos are
    then deleted through the lightbox, reconciliation drops those IDs
    from the snapshot and the pending remainder becomes zero. Those
    photos that were already committed are saved on the server, so the
    group is effectively done — re-rendering it as a fresh, unreviewed
    group would let the user double-assign the already-saved photos
    under a different name. The queue must advance past the group and
    the progress counter must reflect the completed assignment.
    """
    photo_id = live_server["data"]["photos"][0]
    photo_ids = list(range(1, 1002))
    preview = {
        "total": 1001,
        "reviewable": 1001,
        "unresolved": [],
        "skipped": [],
        "groups": [
            {
                "photo_ids": photo_ids,
                "photos": [
                    {
                        "id": photo_id,
                        "filename": "keep.jpg",
                        "latitude": 33.255,
                        "longitude": -116.405,
                        "timestamp": "2026-08-04T10:17:00",
                    },
                ],
                "count": 1001,
                "center": {"lat": 33.255, "lng": -116.405},
                "bounds": {
                    "south": 33.255, "west": -116.405,
                    "north": 33.255, "east": -116.405,
                },
                "spread_m": 0,
                "captured_from": "2026-08-04T10:17:00",
                "captured_to": "2026-08-04T10:17:00",
            },
        ],
    }

    page.route("https://unpkg.com/**", _stub_leaflet)
    page.goto(f"{live_server['url']}/browse")
    page.evaluate(
        "photoId => sessionStorage.setItem('vireoLocationReviewSource', "
        "JSON.stringify({photo_ids: [photoId]}))",
        photo_id,
    )
    page.route(
        "**/api/location-review/preview",
        lambda route: route.fulfill(json=preview),
    )
    page.goto(f"{live_server['url']}/locations/review?source=selection")
    expect(page.locator("#locationReviewGroupTitle")).to_have_text("1,001 photos")

    page.locator("#locationReviewSearch").fill("Drained suffix location")
    page.locator("#locationReviewCustom").click()
    page.evaluate(
        """() => {
          window.__originalSafeFetch = window.safeFetch;
          window.__pending = [];
          window.__requestBodies = [];
          window.safeFetch = function(url, opts, options) {
            if (url !== '/api/batch/location/text') {
              return window.__originalSafeFetch(url, opts, options);
            }
            window.__requestBodies.push(JSON.parse(opts.body));
            return new Promise(function(resolve, reject) {
              window.__pending.push({resolve: resolve, reject: reject});
            });
          };
          window.__settle = function(result) {
            var pending = window.__pending.shift();
            if (result === 'reject') pending.reject(new Error('boom'));
            else pending.resolve({ok: true, updated: result});
          };
        }"""
    )

    page.locator("#locationReviewAssign").click()
    page.wait_for_function("() => window.__pending.length === 1")
    page.evaluate("window.__settle(1000)")  # first chunk (1..1000) commits
    page.wait_for_function("() => window.__pending.length === 1")
    page.evaluate("window.__settle('reject')")  # second chunk [1001] fails

    expect(page.locator("#locationReviewAssign")).to_contain_text(
        "Retry 1 remaining"
    )
    assert page.evaluate("window.__requestBodies.length") == 2

    # Delete the sole remaining pending photo through the lightbox. After
    # reconciliation, photoIds == processedIds so pendingRemaining is 0.
    # Committed photos still sit in group.photos (via the surviving
    # sample), so the buggy code would keep the group in the queue and
    # re-render it as an unreviewed group.
    page.evaluate(
        "document.dispatchEvent(new CustomEvent('lightbox:photodeleted',"
        " {detail: {photoId: 1001}}))"
    )

    # The group must be spliced out of the queue and the empty-state
    # panel shown — the committed photos are saved and must not be
    # available for a second assignment.
    expect(page.locator("#locationReviewEmptyTitle")).to_have_text(
        "All locations reviewed"
    )
    # The completed group must count toward the top-level progress.
    expect(page.locator("#locationReviewProgressText")).to_have_text(
        "1 of 1 assigned"
    )
    # And no further batch requests should have been issued — the retry
    # button no longer exists once the empty state is shown.
    assert page.evaluate("window.__requestBodies.length") == 2


def test_location_review_missing_google_key_links_to_settings(live_server, page):
    """The empty suggestion state explains how to enable nearby places."""
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

    prompt = page.locator(".location-review-setup-prompt")
    expect(prompt).to_contain_text("Enable nearby place suggestions")
    expect(prompt).to_contain_text("Add a Google Maps API key")
    expect(prompt.get_by_role("link", name="Open Settings")).to_have_attribute(
        "href", "/settings#google-maps"
    )
    expect(page.locator("#locationReviewSuggestionStatus")).to_have_text(
        "Setup needed"
    )
    expect(
        page.locator("#locationReviewMapMessage").get_by_role(
            "link", name="Open Settings"
        )
    ).to_have_attribute("href", "/settings#google-maps")


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
    expect(candidates).to_have_count(3)
    expect(candidates.first).to_contain_text("Nearby General Store")
    saved_candidate = candidates.filter(has_text="Far Saved Place")
    expect(saved_candidate.locator(".location-review-candidate-badge")).to_have_text(
        "Previously used"
    )
    expect(saved_candidate.locator(".location-review-candidate-meta")).not_to_contain_text(
        "Saved location"
    )

    coordinate_toggle = page.get_by_label("Show coordinates")
    expect(page.locator('label[for="locationReviewIncludeCoordinates"]')).to_have_attribute(
        "title",
        "Display only. This does not change location assignments or metadata written to XMP.",
    )
    expect(coordinate_toggle).not_to_be_checked()
    expect(candidates.locator(".location-review-candidate-coordinates")).to_have_count(0)

    coordinate_toggle.check()
    expect(candidates.first.locator(".location-review-candidate-coordinates")).to_have_text(
        "32.750000, -117.000000"
    )
    expect(saved_candidate.locator(".location-review-candidate-coordinates")).to_have_text(
        "32.938000, -117.000000"
    )

    coordinate_toggle.uncheck()
    expect(candidates.locator(".location-review-candidate-coordinates")).to_have_count(0)


def test_location_review_filters_nature_and_places_strictly(
    live_server, page,
):
    """Category controls filter one shared candidate pool rather than changing queries."""
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

    recommended_button = page.locator('[data-suggestion-mode="recommended"]')
    nature_button = page.locator('[data-suggestion-mode="nature"]')
    places_button = page.locator('[data-suggestion-mode="places"]')
    expect(recommended_button).to_have_attribute("aria-pressed", "true")
    campground = page.locator(
        ".location-review-candidate", has_text="Tamarisk Grove Campground"
    )
    expect(campground).to_be_visible()
    expect(campground.locator(".location-review-candidate-meta")).to_contain_text(
        "Campground"
    )

    general_store = page.locator(
        ".location-review-candidate", has_text="Nearby General Store"
    )
    expect(general_store).to_be_visible()

    nature_button.click()

    expect(nature_button).to_have_attribute("aria-pressed", "true")
    expect(recommended_button).to_have_attribute("aria-pressed", "false")
    expect(campground).to_be_visible()
    expect(general_store).to_have_count(0)

    places_button.click()

    expect(places_button).to_have_attribute("aria-pressed", "true")
    expect(nature_button).to_have_attribute("aria-pressed", "false")
    expect(general_store).to_be_visible()
    expect(campground).to_have_count(0)

    requested_types = page.evaluate(
        "() => window.__locationReviewNearbyRequests.map(request => request.type)"
    )
    assert "campground" in requested_types
    assert None in requested_types


def test_location_review_groups_hamlets_and_keeps_a_selection_across_filters(
    live_server, page,
):
    """Local areas such as Garrieux are grouped and remain selectable across filters."""
    photo_id = live_server["data"]["photos"][0]
    with live_server["db"].conn:
        live_server["db"].conn.execute(
            "UPDATE photos SET latitude = ?, longitude = ? WHERE id = ?",
            (42.810, 2.940, photo_id),
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

    at_photos = page.locator('[data-candidate-group="at"]')
    expect(at_photos.locator(".location-review-candidate-group-title")).to_have_text(
        "At the photos"
    )
    garrieux = at_photos.locator(
        ".location-review-candidate", has_text="Garrieux"
    )
    expect(garrieux).to_be_visible()
    expect(garrieux.locator(".location-review-candidate-type")).to_have_text(
        "Neighborhood"
    )
    expect(garrieux.locator(".location-review-candidate-detail")).to_contain_text(
        "At photo coordinates"
    )
    expect(
        page.locator('[data-candidate-group="broader"]')
    ).to_contain_text("Pyrénées-Orientales")

    page.locator('[data-suggestion-mode="areas"]').click()
    expect(page.locator(".location-review-candidate")).to_have_count(3)
    expect(page.locator(".location-review-candidate", has_text="Nearby Park")).to_have_count(0)

    garrieux = page.locator(".location-review-candidate", has_text="Garrieux")
    garrieux.click()
    page.locator('[data-suggestion-mode="nature"]').click()

    expect(garrieux).to_be_visible()
    expect(page.locator('[data-candidate-group="selected"]')).to_contain_text(
        "Selected location"
    )
    expect(page.locator(".location-review-candidate", has_text="Nearby Park")).to_be_visible()
    expect(page.locator(".location-review-candidate", has_text="Nearby General Store")).to_have_count(0)


def test_location_review_color_codes_place_types(live_server, page):
    """Type pills make natural places and geographic levels easy to scan."""
    photo_id = live_server["data"]["photos"][0]
    with live_server["db"].conn:
        live_server["db"].conn.execute(
            "UPDATE photos SET latitude = ?, longitude = ? WHERE id = ?",
            (32.750, -117.000, photo_id),
        )

    page.add_init_script("window.__locationReviewIncludeRegions = true")
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

    expected_types = {
        "Nearby Park": ("Park", "nature"),
        "San Diego": ("City or town", "locality"),
        "California": ("State or province", "region"),
        "United States": ("Country", "country"),
    }
    for candidate_name, (type_label, category) in expected_types.items():
        candidate = page.locator(
            ".location-review-candidate", has_text=candidate_name
        )
        type_badge = candidate.locator(".location-review-candidate-type")
        expect(type_badge).to_have_text(type_label)
        expect(type_badge).to_have_class(
            f"location-review-candidate-type "
            f"location-review-candidate-type--{category}"
        )


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


def test_location_review_lightbox_retains_photo_after_trash_failure(
    live_server, page,
):
    """A retryable Trash failure must not emit the photo-deleted event."""
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

    page.evaluate("lightboxDelete()")
    page.evaluate(
        """photoId => {
          var callback = _deleteCallback;
          hideDeleteModal();
          callback({deleted: 0, failed_photo_ids: [photoId]});
        }""",
        photo_ids[0],
    )

    expect(page.locator("#lightboxOverlay")).to_have_class(
        "lightbox-overlay active"
    )
    expect(page.locator("#locationReviewGroupTitle")).to_have_text("2 photos")
    expect(page.locator(".location-review-thumb")).to_have_count(2)
    stored = page.evaluate(
        "() => JSON.parse(sessionStorage.getItem('vireoLocationReviewSource'))"
    )
    assert stored == {"photo_ids": photo_ids}


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

    # Review on Map moved off the slimmed batch bar into the unified More
    # menu (same builder as right-click on a card).
    page.locator("#batchMoreBtn").click()
    item = page.locator(".vireo-ctx-menu .vireo-ctx-item", has_text="Review on Map")
    expect(item).to_be_visible()
    item.click()

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


def test_time_review_assigns_outings_without_inventing_gps(live_server, page):
    photo_ids = live_server["data"]["photos"]
    errors = []
    page.on("pageerror", lambda error: errors.append(str(error)))
    page.goto(f"{live_server['url']}/locations/review?mode=time")
    expect(page.locator("#locationReviewCollection")).to_have_value("all")
    expect(page.locator("#locationReviewGroupTitle")).to_have_text("3 photos")
    expect(page.locator("#locationReviewThumbnails [data-photo-id]")).to_have_count(3)
    expect(page.locator("#locationReviewCaptured")).to_contain_text("2024")
    page.locator("#locationReviewSearch").fill("Morning at the park")
    page.locator("#locationReviewCustom").click()
    with page.expect_response("**/api/batch/location/text") as saved:
        page.locator("#locationReviewAssign").click()
    assert saved.value.ok
    expect(page.locator("#locationReviewGroupTitle")).to_have_text("2 photos")
    expect(page.locator(".location-review-candidate", has_text="Morning at the park")).to_be_visible()
    db = live_server["db"]
    rows = db.conn.execute(
        "SELECT pk.photo_id FROM photo_keywords pk JOIN keywords k ON k.id = pk.keyword_id "
        "WHERE k.name = 'Morning at the park'"
    ).fetchall()
    assert {row[0] for row in rows} == set(photo_ids[:3])
    assert all(row[0] is None and row[1] is None for row in db.conn.execute(
        "SELECT latitude, longitude FROM photos"
    ))
    page.reload()
    expect(page.locator("#locationReviewGroupTitle")).to_have_text("2 photos")
    assert errors == []


def test_time_review_splits_and_reviews_individual_photos(live_server, page):
    photo_ids = live_server["data"]["photos"]
    page.goto(f"{live_server['url']}/locations/review?mode=time")
    page.locator("#locationReviewShowAll").click()
    page.locator(f'[data-split-before="{photo_ids[2]}"]').click()
    expect(page.locator("#locationReviewGroupTitle")).to_have_text("2 photos")
    expect(page.locator("#locationReviewRemaining")).to_have_text("3 in queue")
    page.locator(f'[data-review-separately="{photo_ids[1]}"]').click()
    expect(page.locator("#locationReviewGroupTitle")).to_have_text("1 photo")
    expect(page.locator("#locationReviewRemaining")).to_have_text("4 in queue")
    page.locator("#locationReviewSearch").fill("Just this photo")
    page.locator("#locationReviewCustom").click()
    with page.expect_response("**/api/batch/location/text") as saved:
        page.locator("#locationReviewAssign").click()
    assert saved.value.request.post_data_json["photo_ids"] == [photo_ids[0]]
    expect(page.locator(f'[data-photo-id="{photo_ids[2]}"]')).to_be_visible()
    page.locator("#locationReviewNext").click()
    expect(page.locator("#locationReviewGroupTitle")).to_have_text("2 photos")
    page.locator("#locationReviewNext").click()
    expect(page.locator(f'[data-photo-id="{photo_ids[1]}"]')).to_be_visible()


def test_time_review_reuses_saved_location_and_preserves_scope(live_server, page):
    db = live_server["db"]
    photo_ids = live_server["data"]["photos"]
    location_id = db.get_or_create_text_location("Lakeside Park")
    db.conn.execute("UPDATE keywords SET latitude = 42, longitude = -71 WHERE id = ?", (location_id,))
    db.set_photo_location(photo_ids[3], location_id)
    collection_id = db.add_collection("Morning outing", json.dumps([
        {"field": "photo_ids", "value": photo_ids[:3]},
    ]))
    page.goto(f"{live_server['url']}/locations/review?collection_id={collection_id}")
    page.locator("#locationReviewMode").select_option("time")
    expect(page.locator("#locationReviewGroupTitle")).to_have_text("3 photos")
    assert parse_qs(urlparse(page.url).query)["collection_id"] == [str(collection_id)]
    page.locator("#locationReviewSearch").fill("Lakeside")
    page.locator(".location-review-candidate", has_text="Lakeside Park").click()
    with page.expect_response("**/api/batch/location") as saved:
        page.locator("#locationReviewAssign").click()
    assert saved.value.ok
    assert saved.value.request.post_data_json == {"photo_ids": photo_ids[:3], "keyword_id": location_id}
    expect(page.locator("#locationReviewEmptyTitle")).to_have_text("All locations reviewed")
    assert db.conn.execute("SELECT COUNT(*) FROM photo_keywords WHERE keyword_id = ?", (location_id,)).fetchone()[0] == 4


def test_time_review_samples_whole_group_and_locks_edits_during_retry(live_server, page):
    photos = [{"id": index, "filename": f"photo-{index}.jpg", "companion_path": None,
               "timestamp": "2026-08-01T10:00:00", "latitude": None, "longitude": None}
              for index in range(1, 1002)]
    preview = {"total": 1001, "reviewable": 1001, "skipped": [], "unresolved": [], "groups": [{
        "id": "time-1", "grouping": "time", "count": 1001,
        "photo_ids": [photo["id"] for photo in photos], "photos": photos,
        "center": None, "bounds": None, "spread_m": None,
        "captured_from": photos[0]["timestamp"], "captured_to": photos[-1]["timestamp"],
    }]}
    page.route("**/api/location-review/preview", lambda route: route.fulfill(json=preview))
    requests = []

    def assign(route):
        requests.append(route.request.post_data_json)
        if len(requests) == 2:
            route.fulfill(status=500, json={"error": "Interrupted assignment"})
        else:
            route.fulfill(json={"ok": True})

    page.route("**/api/batch/location/text", assign)
    page.goto(f"{live_server['url']}/locations/review?mode=time")
    expect(page.locator("#locationReviewThumbnails [data-photo-id]")).to_have_count(6)
    expect(page.locator('[data-photo-id="1"]')).to_be_visible()
    expect(page.locator('[data-photo-id="1001"]')).to_be_visible()
    page.locator("#locationReviewShowAll").click()
    expect(page.locator("#locationReviewThumbnails [data-photo-id]")).to_have_count(36)
    page.locator("#locationReviewPageNext").click()
    expect(page.locator('[data-photo-id="37"]')).to_be_visible()
    page.locator("#locationReviewSearch").fill("Large outing")
    page.locator("#locationReviewCustom").click()
    page.locator("#locationReviewAssign").click()
    expect(page.locator("#locationReviewAssign")).to_have_text("Retry 1 remaining")
    for selector in ["#locationReviewMode", "#locationReviewGap", "#locationReviewCollection",
                     "#locationReviewSkip", "#locationReviewShowAll", "#locationReviewPagePrevious"]:
        expect(page.locator(selector)).to_be_disabled()
    expect(page.locator('[data-split-before="37"]')).to_be_disabled()
    expect(page.locator('[data-review-separately="37"]')).to_be_disabled()
    page.locator("#locationReviewAssign").click()
    expect(page.locator("#locationReviewEmptyTitle")).to_have_text("All locations reviewed")
    assert [len(request["photo_ids"]) for request in requests] == [1000, 1, 1]
    assert requests[-1]["photo_ids"] == [1001]
    assert all("latitude" not in request and "longitude" not in request for request in requests)


def test_time_review_does_not_expand_an_expired_selection(live_server, page):
    page.goto(f"{live_server['url']}/locations/review?mode=time&source=selection")
    expect(page.locator("#locationReviewEmptyTitle")).to_have_text("Choose a collection")
    expect(page.locator("#locationReviewCollection")).to_have_value("")
    expect(page.locator("#locationReviewGroupTitle")).not_to_be_visible()
    page.locator("#locationReviewGap").select_option("15")
    expect(page.locator("#locationReviewGap")).to_have_value("60")
    page.locator("#locationReviewMode").select_option("coordinates")
    expect(page.locator("#locationReviewMode")).to_have_value("time")
    expect(page.locator("#locationReviewEmptyTitle")).to_have_text("Choose a collection")
    assert parse_qs(urlparse(page.url).query) == {"mode": ["time"], "source": ["selection"]}
    page.locator("#locationReviewCollection").select_option("all")
    expect(page.locator("#locationReviewGroupTitle")).to_have_text("3 photos")


def test_browse_opens_time_review_for_selected_photos(live_server, page):
    photo_ids = live_server["data"]["photos"][:2]
    page.goto(f"{live_server['url']}/browse")
    page.locator(f'.grid-card[data-id="{photo_ids[0]}"]').click()
    page.locator(f'.grid-card[data-id="{photo_ids[1]}"]').click(modifiers=["Meta"])
    page.locator(f'.grid-card[data-id="{photo_ids[1]}"]').click(button="right")
    page.get_by_text("Add Locations by Capture Time", exact=True).click()
    expect(page.locator("#locationReviewGroupTitle")).to_have_text("2 photos")
    expect(page.locator("#locationReviewCollection")).to_have_value("__selection__")
    assert parse_qs(urlparse(page.url).query) == {"source": ["selection"], "mode": ["time"]}
    page.locator("#locationReviewGap").select_option("15")
    expect(page.locator("#locationReviewGroupTitle")).to_have_text("2 photos")
    expect(page.locator("#locationReviewCollection")).to_have_value("__selection__")
