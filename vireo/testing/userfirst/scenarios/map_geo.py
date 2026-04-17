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
        # (template regression we must surface).  Probe the script URL
        # and classify the response:
        #   - 2xx: reachable but L undefined → real bug, keep findings
        #   - 4xx (except 408/429): wrong URL in template → fail
        #   - 408/429/5xx: transient CDN issue → tolerate
        #   - no response (network error): CDN unreachable → tolerate
        probe_status = None
        with contextlib.suppress(Exception):
            probe_status = session.page.request.fetch(
                leaflet_src, method="GET", timeout=5000
            ).status

        def _suppress_leaflet_findings():
            session.report.findings = [
                f
                for f in session.report.findings
                if not (f.kind == "BUG" and "L is not defined" in f.message)
            ]

        if probe_status is None:
            _suppress_leaflet_findings()  # network failure → CDN outage
        elif 200 <= probe_status < 300:
            pass  # reachable but L undefined — leave findings as-is
        elif probe_status in (408, 429) or 500 <= probe_status < 600:
            # Transient CDN condition (timeout, rate limit, server error).
            # Template is still correct; don't fail on external availability.
            _suppress_leaflet_findings()
        else:
            # 4xx other than 408/429 — the URL in the template is wrong
            # (e.g. 404 from a typo'd path or deleted CDN version).  This
            # is a genuine template regression worth surfacing.
            session.assert_that(
                False,
                f"Leaflet script URL returned HTTP {probe_status}: {leaflet_src}",
            )

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
