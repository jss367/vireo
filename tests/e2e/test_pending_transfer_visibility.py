"""Dashboard and Storage surface local originals and distinguish unknown status."""
import os
from pathlib import Path

import pytest
from playwright.sync_api import expect


def test_pending_transfer_panel_collapses_and_remembers_choice(live_server, page):
    item = {"id": "local", "name": "Photos from the coast", "state": "ready",
            "destination": "/Volumes/Photos/Coast", "photo_count": 2549,
            "cataloged_bytes": 42 * 2**30, "unsynced_photos": 2545}
    page.route("**/api/import/pending-archives", lambda route: route.fulfill(json={"items": [item]}))
    page.goto(live_server["url"] + "/browse")
    panel = page.locator("#pendingArchives")
    toggle = panel.locator("#pendingArchivesToggle")
    content = panel.locator("#pendingArchivesContent")
    expect(panel).to_be_visible()
    expect(toggle).to_have_attribute("aria-expanded", "false")
    expect(toggle).to_contain_text("1 pending NAS transfer")
    expect(content).to_be_hidden()
    assert panel.bounding_box()["height"] < 60
    if directory := os.environ.get("VIREO_TRANSFER_SCREENSHOTS"):
        target = Path(directory)
        target.mkdir(parents=True, exist_ok=True)
        page.screenshot(path=str(target / "pending-transfers-collapsed.png"))

    toggle.focus()
    toggle.press("Enter")
    expect(toggle).to_have_attribute("aria-expanded", "true")
    expect(content).to_be_visible()
    expect(panel.get_by_role("button", name="Sync metadata and send to NAS")).to_be_visible()
    page.reload()
    expect(content).to_be_visible()
    toggle.press("Space")
    expect(content).to_be_hidden()

    # Background refreshes must not reopen the panel; warnings remain discoverable.
    item["error"] = "NAS disconnected"
    page.evaluate("window.dispatchEvent(new Event('focus'))")
    expect(toggle).to_contain_text("Needs attention")
    expect(content).to_be_hidden()
    page.goto(live_server["url"] + "/dashboard")
    expect(panel).to_be_visible()
    expect(toggle).to_have_attribute("aria-expanded", "false")
    expect(content).to_be_hidden()
    page.set_viewport_size({"width": 390, "height": 844})
    toggle.click()
    expect(content).to_be_visible()
    expect(panel).to_contain_text("NAS disconnected")
    assert toggle.evaluate("el => el.scrollWidth <= el.clientWidth")


@pytest.mark.parametrize("path", ["/dashboard", "/storage"])
def test_pending_transfer_failure_is_visible_and_recovers_to_empty(live_server, page, path):
    state = {"failed": True}

    def pending(route):
        route.fulfill(status=503 if state["failed"] else 200,
                      json={"error": "unavailable"} if state["failed"] else {"items": []})

    page.route("**/api/import/pending-archives", pending)
    page.goto(live_server["url"] + path)
    panel = page.locator("#pendingArchives")
    expect(panel).to_be_visible()
    expect(panel.locator("#pendingArchiveError")).to_have_text("Could not check photos waiting for NAS transfer.")
    state["failed"] = False
    page.evaluate("window.dispatchEvent(new Event('focus'))")
    expect(panel).to_be_hidden()
    # A failure after a previously empty success must not be mistaken for empty;
    # recovery to the unchanged empty response must hide the warning again.
    state["failed"] = True
    page.evaluate("window.dispatchEvent(new Event('focus'))")
    expect(panel).to_be_visible()
    state["failed"] = False
    page.evaluate("window.dispatchEvent(new Event('focus'))")
    expect(panel).to_be_hidden()


@pytest.mark.parametrize("path", ["/dashboard", "/storage"])
def test_pending_transfer_status_and_source_availability(live_server, page, path):
    page.set_viewport_size({"width": 1280, "height": 1000})
    item = {"id": "waiting", "name": "October wildlife photos", "destination": "/Volumes/Photos/2026/October",
            "state": "ready", "source_available": True, "photo_count": 3159,
            "cataloged_bytes": 48 * 2**30, "created_at": "2026-10-17 12:00:00"}
    page.route("**/api/import/pending-archives", lambda route: route.fulfill(json={"items": [item]}))
    page.add_init_script("Date.now = () => Date.UTC(2026, 9, 24, 12)")
    page.goto(live_server["url"] + path)
    panel = page.locator("#pendingArchives")
    expect(panel).to_contain_text("Kept locally for 7 days")
    expect(panel).to_contain_text("3,159 cataloged photos")
    expect(panel).to_contain_text("48 GB estimated")
    expect(panel).to_contain_text("These local originals may be your only copies")
    panel.get_by_role("button", name="Show details").click()
    expect(panel.locator(".pending-archive-destination")).to_be_visible()
    send = panel.get_by_role("button", name="Send to NAS", exact=True)
    expect(send).to_be_enabled()
    if directory := os.environ.get("VIREO_TRANSFER_SCREENSHOTS"):
        target = Path(directory)
        target.mkdir(parents=True, exist_ok=True)
        page.screenshot(path=str(target / (path.strip('/') + '.png')))
    item["state"] = "sending"
    page.evaluate("window.dispatchEvent(new Event('focus'))")
    expect(panel.get_by_role("button", name="Sending to NAS…", exact=True)).to_be_disabled()
    item["state"] = "ready"
    item["source_available"] = False
    item["cataloged_bytes"] = None
    page.evaluate("window.dispatchEvent(new Event('focus'))")
    expect(send).to_be_disabled()
    expect(panel).to_contain_text("Local originals unavailable")
    expect(panel).to_contain_text("Size unavailable")
