import pytest
from playwright.sync_api import expect

# Every user-facing page route in the app.
# /map is excluded: it loads Leaflet from unpkg.com CDN, so network
# latency would cause CI flakes unrelated to Vireo itself.
PAGES = [
    "/browse",
    "/review",
    "/lightroom",
    "/audit",
    "/cull",
    "/pipeline",
    "/pipeline/review",
    "/pipeline/rapid-review",
    "/variants",
    "/workspace",
    "/compare",
    "/storage",
    "/settings",
    "/shortcuts",
    "/keywords",
    "/jobs",
    "/move",
    "/highlights",
    "/dashboard",
    "/logs",
]


@pytest.mark.parametrize("path", PAGES)
def test_page_loads_within_timeout(live_server, page, path):
    """Every page must return 200 and finish loading within 5 seconds."""
    url = live_server["url"]
    response = page.goto(f"{url}{path}", timeout=5000)
    assert response is not None
    assert response.status == 200
    # Page should have rendered — title or body content present
    expect(page.locator("body")).not_to_be_empty()


def test_storage_page_reports_health_and_safety(live_server, page):
    separate_mask_requests = []
    page.on(
        "request",
        lambda request: separate_mask_requests.append(request.url)
        if request.url.endswith("/api/storage/masks") else None,
    )
    page.goto(f"{live_server['url']}/storage")

    expect(page.locator("#storageTotalSize")).not_to_have_text("-")
    expect(page.locator("#storageFreeSize")).not_to_have_text("-")
    expect(page.locator("#storageReclaimableSize")).not_to_have_text("-")
    expect(page.locator("#storageUpdatedAt")).to_contain_text("Updated")
    expect(page.locator("#storageLocations code").first).not_to_have_text("")
    expect(page.locator("#storageGrid .stat-label", has_text="Database")).to_have_count(1)
    expect(page.get_by_text("Safe to clear", exact=True).first).to_be_visible()
    expect(page.get_by_role("button", name="Open folder").first).to_be_visible()
    assert separate_mask_requests == []


def _expect_text_correction_disabled(locator):
    expect(locator).to_have_attribute("autocomplete", "off")
    expect(locator).to_have_attribute("autocorrect", "off")
    expect(locator).to_have_attribute("autocapitalize", "none")
    expect(locator).to_have_attribute("spellcheck", "false")


def test_text_inputs_disable_os_correction_app_wide(live_server, page):
    """Text/search fields should not let browser or OS autocorrect alter queries."""
    url = live_server["url"]
    page.goto(f"{url}/edit")

    _expect_text_correction_disabled(page.locator("#editorSearchInput"))

    page.evaluate(
        """() => {
            const input = document.createElement('input');
            input.type = 'text';
            input.id = 'dynamicTextInput';
            document.body.appendChild(input);
        }"""
    )
    _expect_text_correction_disabled(page.locator("#dynamicTextInput"))

    # A password input flipped to text at runtime (Settings "Show" buttons)
    # should also receive the correction opt-out.
    page.evaluate(
        """() => {
            const input = document.createElement('input');
            input.type = 'password';
            input.id = 'flippedPasswordInput';
            document.body.appendChild(input);
            input.type = 'text';
        }"""
    )
    _expect_text_correction_disabled(page.locator("#flippedPasswordInput"))
