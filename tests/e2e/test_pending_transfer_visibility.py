"""Dashboard and Storage surface local originals and distinguish unknown status."""
import os
from pathlib import Path

import pytest
from playwright.sync_api import expect


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
