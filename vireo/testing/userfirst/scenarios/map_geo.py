"""Scenario: visit the map/geo view page.

Verifies that /map renders, the Leaflet map container exists, filter
controls are present, and the appropriate "no geolocated photos" message
is shown when the seed data has no GPS coordinates.
"""
import contextlib
import re
from urllib.parse import urlparse

# Hosts we trust to be Leaflet CDNs.  When the script URL points at one
# of these and the probe fails (no response / DNS failure), assume a
# transient outage rather than a template regression.  Anything else is
# treated as a bad URL in the template.
_KNOWN_LEAFLET_CDN_HOSTS = frozenset({
    "unpkg.com",
    "cdn.jsdelivr.net",
    "cdnjs.cloudflare.com",
})


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
        #   - 2xx: reachable but L undefined → real bug, fail explicitly
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
            # Network-level failure (DNS, connection refused, timeout
            # before HTTP).  Only treat this as a CDN outage when the
            # URL points at a known Leaflet CDN; otherwise the template
            # was edited to point at an unresolvable host (e.g. typo'd
            # or removed CDN), which is a real regression.
            host = (urlparse(leaflet_src).hostname or "").lower()
            if host in _KNOWN_LEAFLET_CDN_HOSTS:
                _suppress_leaflet_findings()
            else:
                session.assert_that(
                    False,
                    "Leaflet script URL unreachable and host is not a known "
                    f"CDN ({host!r}): {leaflet_src}",
                )
        elif 200 <= probe_status < 300:
            # Script URL is reachable but L is undefined — this is a real
            # regression (e.g. map init logic removed, the URL now serves
            # non-Leaflet content, or the script errors during execution).
            # Fail explicitly rather than relying on a downstream "L is not
            # defined" finding, which may be absent if the page defensively
            # guards against the missing global.
            session.assert_that(
                False,
                f"Leaflet script reachable at {leaflet_src} "
                f"(HTTP {probe_status}) but window.L is undefined — "
                "map initialization regression",
            )
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
        # The status bar either shows the explicit "No geolocated photos
        # found" message or "Showing 0 of 0 geolocated photos | 0% GPS
        # coverage".  Match those exact zero shapes — a substring check
        # for "0%" would also match "100% GPS coverage", and "Showing 0"
        # would match "Showing 0 of 100" (visible-after-filtering, not a
        # zero-GPS state).
        zero_gps_status = (
            "No geolocated" in status_text
            or (
                re.search(r"\bShowing 0 of 0\b", status_text) is not None
                and re.search(r"(?<!\d)0% GPS coverage\b", status_text) is not None
            )
        )
        session.assert_that(
            zero_gps_status,
            "expected zero-GPS status ('No geolocated' message or "
            f"'Showing 0 of 0' + '0% GPS coverage'), got {status_text!r}",
        )

        sidebar_text = session.eval(
            "(document.getElementById('sidebarHeader') || {}).textContent || ''"
        )
        # Require an exact zero count — "0 photo" as a substring also
        # matches "10 photos", "20 photos", etc.  Use a leading
        # non-digit boundary plus the trailing word boundary.
        session.assert_that(
            re.search(r"(?<!\d)0 photos?\b", sidebar_text) is not None,
            f"expected exactly '0 photos' in sidebar header, got {sidebar_text!r}",
        )

    session.screenshot("map-no-gps-data")
