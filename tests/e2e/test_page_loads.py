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
    "/variants",
    "/workspace",
    "/compare",
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
