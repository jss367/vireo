"""E2E tests for the photo detail Location section in browse.html.

These tests exercise the section in the no-Google-API-key branch (the live_server
fixture starts with an empty config), so they cover the free-text Enter path
and the server-side initial render. They do NOT test Google Places autocomplete
itself — that requires a real key and a network round-trip we don't want in CI.
"""

from playwright.sync_api import expect


def test_location_section_renders_empty(live_server, page):
    """Opening a photo with no location shows the empty input."""
    url = live_server["url"]
    page.goto(f"{url}/browse")

    first = page.locator(".grid-card").first
    first.wait_for(state="visible")
    first.click()

    section = page.locator("#locationSection")
    expect(section).to_be_visible()
    expect(page.locator("#locationInput")).to_be_visible()
    expect(page.locator("#locationFilled")).to_be_hidden()


def test_freetext_enter_creates_location(live_server, page):
    """Typing a free-text location and pressing Enter swaps to the filled state."""
    url = live_server["url"]
    page.goto(f"{url}/browse")

    first = page.locator(".grid-card").first
    first.wait_for(state="visible")
    first.click()

    inp = page.locator("#locationInput")
    inp.wait_for(state="visible")
    inp.fill("the meadow behind the cabin")
    inp.press("Enter")

    filled = page.locator("#locationFilled")
    expect(filled).to_be_visible()
    expect(page.locator("#locationFilled .filled-place")).to_have_text(
        "the meadow behind the cabin"
    )
    # Free-text locations have no parent chain.
    expect(page.locator("#locationFilled .filled-parents")).to_have_count(0)
    expect(page.locator("#locationEmpty")).to_be_hidden()


def test_clear_button_returns_to_empty(live_server, page):
    """The × button on the filled state clears the location server-side."""
    url = live_server["url"]
    page.goto(f"{url}/browse")

    first = page.locator(".grid-card").first
    first.wait_for(state="visible")
    first.click()

    # Set a location first.
    inp = page.locator("#locationInput")
    inp.wait_for(state="visible")
    inp.fill("test location")
    inp.press("Enter")

    expect(page.locator("#locationFilled")).to_be_visible()

    # Clear it.
    page.locator("#locationFilled .clear-location").click()

    expect(page.locator("#locationFilled")).to_be_hidden()
    expect(page.locator("#locationEmpty")).to_be_visible()


def test_location_persists_across_reload(live_server, page):
    """Server-side initial render: re-opening the photo after a reload shows the
    saved location without any client-side roundtrip beyond /api/photos/<id>."""
    url = live_server["url"]
    page.goto(f"{url}/browse")

    first = page.locator(".grid-card").first
    first.wait_for(state="visible")
    first.click()

    inp = page.locator("#locationInput")
    inp.wait_for(state="visible")
    inp.fill("Persistent Place")
    inp.press("Enter")

    expect(page.locator("#locationFilled .filled-place")).to_have_text(
        "Persistent Place"
    )

    # Reload the whole page; the section should re-render filled when the
    # photo is reopened.
    page.reload()
    page.locator(".grid-card").first.wait_for(state="visible")
    page.locator(".grid-card").first.click()

    expect(page.locator("#locationFilled")).to_be_visible()
    expect(page.locator("#locationFilled .filled-place")).to_have_text(
        "Persistent Place"
    )


def test_no_gmaps_script_when_key_empty(live_server, page):
    """With no API key configured, focusing the input must NOT inject the
    Google Maps script tag — we shouldn't ping Google with an empty key."""
    url = live_server["url"]
    page.goto(f"{url}/browse")

    first = page.locator(".grid-card").first
    first.wait_for(state="visible")
    first.click()

    inp = page.locator("#locationInput")
    inp.wait_for(state="visible")
    inp.focus()

    # Give any binding a moment to fire (sync; no real network expected).
    page.wait_for_timeout(100)

    # No <script src> pointing at maps.googleapis.com should exist.
    count = page.evaluate(
        "document.querySelectorAll('script[src*=\"maps.googleapis.com\"]').length"
    )
    assert count == 0
