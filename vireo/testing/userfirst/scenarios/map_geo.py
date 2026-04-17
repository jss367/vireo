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
    # "ReferenceError: L is not defined" when the Leaflet global is missing.
    # Only suppress that specific signature — broader patterns like "leaflet"
    # would hide real map regressions where Vireo feeds bad data to Leaflet.
    session.page.wait_for_timeout(2000)
    session.report.findings = [
        f
        for f in session.report.findings
        if not (
            f.kind == "BUG"
            and "L is not defined" in f.message
        )
    ]

    session.screenshot("map-initial")

    # Check whether Leaflet actually loaded.  If the CDN was unavailable,
    # the map JS throws before loadPhotos() runs, so status/sidebar remain
    # at their initial "Loading..." text.  In that case, only assert on the
    # static HTML (container, filters) — not on JS-populated content.
    leaflet_loaded = session.eval("typeof L !== 'undefined'")

    # --- Static HTML assertions (always valid) ---

    # The map container div should exist
    has_map = session.eval("!!document.getElementById('map')")
    session.assert_that(has_map, "expected #map container on map page")

    # Filter controls should be present
    for elem_id, label in [
        ("filterFolder", "folder filter dropdown"),
        ("filterRating", "rating filter dropdown"),
        ("filterSpecies", "species filter dropdown"),
        ("filterKeyword", "keyword filter input"),
        ("filterDateFrom", "date-from filter"),
        ("filterDateTo", "date-to filter"),
    ]:
        present = session.eval(f"!!document.getElementById('{elem_id}')")
        session.assert_that(present, f"expected {label}")

    # The sidebar header and status bar elements should exist in the DOM
    has_sidebar_header = session.eval("!!document.getElementById('sidebarHeader')")
    session.assert_that(has_sidebar_header, "expected sidebar header on map page")

    has_status = session.eval("!!document.getElementById('mapStatus')")
    session.assert_that(has_status, "expected map status bar")

    # --- JS-populated assertions (only when Leaflet loaded) ---

    if leaflet_loaded:
        # Without GPS data, the status should indicate no geolocated photos
        status_text = session.eval(
            "(document.getElementById('mapStatus') || {}).textContent || ''"
        )
        session.assert_that(
            "No geolocated" in status_text
            or "0%" in status_text
            or "Showing 0" in status_text,
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
