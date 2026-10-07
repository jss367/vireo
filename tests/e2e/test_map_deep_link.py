from playwright.sync_api import expect

from e2e.leaflet_stub import stub_leaflet


def test_map_photo_id_deep_link_focuses_marker(live_server, page):
    """The map page zooms to and opens the marker named by ?photo_id=."""
    pid = live_server["data"]["photos"][0]
    live_server["db"].conn.execute(
        "UPDATE photos SET latitude = ?, longitude = ? WHERE id = ?",
        (37.7749, -122.4194, pid),
    )
    live_server["db"].conn.commit()

    page.route("https://unpkg.com/**", stub_leaflet)
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

    page.route("https://unpkg.com/**", stub_leaflet)
    page.goto(f"{live_server['url']}/map?photo_id={pid}")

    expect(page.locator("#mapStatus")).to_contain_text("No map location found for this photo.")


def test_empty_map_links_to_photos_without_coordinates(live_server, page):
    """An all-empty map gives users a working path to the affected photos."""
    page.route("https://unpkg.com/**", stub_leaflet)
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

    page.route("https://unpkg.com/**", stub_leaflet)
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

    page.route("https://unpkg.com/**", stub_leaflet)
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


def test_map_selection_shows_only_selected_photos_and_accounts_for_the_rest(
    live_server, page
):
    """A multi-photo View on Map plots just the selection and explains omissions."""
    located, other_located, unlocated = live_server["data"]["photos"][:3]
    for pid, lat, lng in ((located, 37.7749, -122.4194),
                          (other_located, 40.7128, -74.0060)):
        live_server["db"].conn.execute(
            "UPDATE photos SET latitude = ?, longitude = ? WHERE id = ?",
            (lat, lng, pid),
        )
    live_server["db"].conn.commit()

    page.route("https://unpkg.com/**", stub_leaflet)
    page.add_init_script(
        "sessionStorage.setItem('vireoMapSelection', "
        f"JSON.stringify({{photo_ids: [{located}, {unlocated}]}}))"
    )
    page.goto(f"{live_server['url']}/map?source=selection")

    status = page.locator("#mapStatus")
    expect(status).to_contain_text("Showing 1 of 2 selected photos")
    expect(status).to_contain_text("1 without a location")
    expect(status.locator("a")).to_have_attribute("href", "/map")
    expect(page.locator(".sidebar-card")).to_have_count(1)
    expect(page.locator(f".sidebar-card[data-id='{located}']")).to_be_visible()
    assert page.evaluate("window.__mapMarkerCount") == 1
    assert page.evaluate("!!window.__lastFitBounds")
