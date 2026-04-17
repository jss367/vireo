"""Scenario: visit the map/geo view page.

Verifies that /map renders, the Leaflet map container exists, filter
controls are present, and the appropriate "no geolocated photos" message
is shown when the seed data has no GPS coordinates.
"""


def run(session):
    session.goto("/map")

    # The map page loads Leaflet from unpkg.com.  The harness only records
    # context.url for same-origin requests, so CDN failures won't appear
    # there.  The real offline failure mode is a downstream console error
    # like "ReferenceError: L is not defined" when the Leaflet global is
    # missing.  Filter both patterns so CDN outages don't flag as Vireo bugs.
    session.page.wait_for_timeout(2000)
    session.report.findings = [
        f
        for f in session.report.findings
        if not (
            f.kind == "BUG"
            and (
                "L is not defined" in f.message
                or "leaflet" in f.message.lower()
            )
        )
    ]

    session.screenshot("map-initial")

    # The map container div should exist
    has_map = session.eval("!!document.getElementById('map')")
    session.assert_that(has_map, "expected #map container on map page")

    # Filter controls should be present
    has_folder_filter = session.eval("!!document.getElementById('filterFolder')")
    session.assert_that(has_folder_filter, "expected folder filter dropdown")

    has_rating_filter = session.eval("!!document.getElementById('filterRating')")
    session.assert_that(has_rating_filter, "expected rating filter dropdown")

    has_species_filter = session.eval("!!document.getElementById('filterSpecies')")
    session.assert_that(has_species_filter, "expected species filter dropdown")

    has_keyword_filter = session.eval("!!document.getElementById('filterKeyword')")
    session.assert_that(has_keyword_filter, "expected keyword filter input")

    has_date_from = session.eval("!!document.getElementById('filterDateFrom')")
    session.assert_that(has_date_from, "expected date-from filter")

    has_date_to = session.eval("!!document.getElementById('filterDateTo')")
    session.assert_that(has_date_to, "expected date-to filter")

    # The sidebar should exist with a photo count header
    has_sidebar_header = session.eval("!!document.getElementById('sidebarHeader')")
    session.assert_that(has_sidebar_header, "expected sidebar header on map page")

    # The map status bar should exist
    has_status = session.eval("!!document.getElementById('mapStatus')")
    session.assert_that(has_status, "expected map status bar")

    # Without GPS data, the status should indicate no geolocated photos
    status_text = session.eval(
        "(document.getElementById('mapStatus') || {}).textContent || ''"
    )
    session.assert_that(
        "No geolocated" in status_text or "0%" in status_text or "Showing 0" in status_text,
        f"expected 'no geolocated' or '0%' in status, got {status_text!r}",
    )

    # The sidebar should show "0 photos"
    sidebar_text = session.eval(
        "(document.getElementById('sidebarHeader') || {}).textContent || ''"
    )
    session.assert_that(
        "0 photo" in sidebar_text,
        f"expected '0 photos' in sidebar header, got {sidebar_text!r}",
    )

    session.screenshot("map-no-gps-data")
