from playwright.sync_api import expect

LEAFLET_STUB = """
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
    return {
      latlng: latlng,
      addTo: function() { return this; },
      bindTooltip: function() { return this; },
      on: function() { return this; }
    };
  }
};
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
