"""Scenario: visit the map/geo view page.

Verifies that /map renders, the Leaflet map container exists, filter
controls are present, and the appropriate "no geolocated photos" message
is shown when the seed data has no GPS coordinates.
"""
import contextlib


def run(session):
    session.goto("/map")
    session.page.wait_for_timeout(2000)

    session.screenshot("map-initial")

    # Distinguish CDN outage from template regression.  Match the core
    # Leaflet script specifically, not any src containing "leaflet" —
    # map.html also loads leaflet.markercluster.js which would match a
    # broader selector even if the primary leaflet.js is removed.
    leaflet_src = session.eval(
        """(() => {
            const s = document.querySelector(
                'script[src*="/leaflet.js"], script[src*="/leaflet.min.js"]'
            );
            return s ? s.src : null;
        })()"""
    )
    leaflet_loaded = session.eval("typeof L !== 'undefined'")

    if leaflet_src is None:
        # Template regression: Leaflet script tag removed from map.html
        session.assert_that(False, "Leaflet <script> tag missing from map.html")
    elif not leaflet_loaded:
        # Script tag present but L undefined — could be a CDN outage
        # (tolerate) or a broken URL / wrong version in map.html
        # (template regression we must surface).  Probe the script URL:
        # a non-2xx response means the URL is wrong, a network failure
        # means the CDN is unreachable, and a 2xx with L undefined is
        # a real bug worth surfacing.
        probe_status = None
        with contextlib.suppress(Exception):
            probe_status = session.page.request.fetch(
                leaflet_src, method="GET", timeout=5000
            ).status

        if probe_status is not None and 200 <= probe_status < 300:
            pass  # reachable but L undefined — leave findings as-is
        elif probe_status is not None:
            session.assert_that(
                False,
                f"Leaflet script URL returned HTTP {probe_status}: {leaflet_src}",
            )
        else:
            # CDN unreachable — suppress the "L is not defined" BUG
            # findings it produced; they're not our regression.
            session.report.findings = [
                f
                for f in session.report.findings
                if not (f.kind == "BUG" and "L is not defined" in f.message)
            ]

    # --- Static HTML assertions (always valid) ---

    has_map = session.eval("!!document.getElementById('map')")
    session.assert_that(has_map, "expected #map container on map page")

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

    has_sidebar_header = session.eval("!!document.getElementById('sidebarHeader')")
    session.assert_that(has_sidebar_header, "expected sidebar header on map page")

    has_status = session.eval("!!document.getElementById('mapStatus')")
    session.assert_that(has_status, "expected map status bar")

    # --- JS-populated assertions (only when Leaflet loaded successfully) ---

    if leaflet_loaded:
        status_text = session.eval(
            "(document.getElementById('mapStatus') || {}).textContent || ''"
        )
        session.assert_that(
            "No geolocated" in status_text
            or "0%" in status_text
            or "Showing 0" in status_text,
            f"expected 'no geolocated' or '0%' in status, got {status_text!r}",
        )

        sidebar_text = session.eval(
            "(document.getElementById('sidebarHeader') || {}).textContent || ''"
        )
        session.assert_that(
            "0 photo" in sidebar_text,
            f"expected '0 photos' in sidebar header, got {sidebar_text!r}",
        )

    session.screenshot("map-no-gps-data")
