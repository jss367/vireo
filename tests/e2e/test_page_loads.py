import pytest
from playwright.sync_api import expect

# Every user-facing page route in the app.
# /map is excluded: it loads Leaflet from unpkg.com CDN, so network
# latency would cause CI flakes unrelated to Vireo itself.
PAGES = [
    "/browse",
    "/review",
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
