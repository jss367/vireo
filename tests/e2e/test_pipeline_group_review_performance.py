"""Keep comparison previews responsive while preserving original inspection."""

import io

import pytest
from PIL import Image
from playwright.sync_api import expect


def _open_review(page, live_server):
    photos = [
        {"id": pid, "filename": f"bird-{pid}.jpg", "label": "REVIEW", "width": 6000, "height": 4000}
        for pid in range(1, 4)
    ]
    results = {
        "photos": photos,
        "encounters": [{
            "photo_ids": [1, 2, 3], "photo_count": 3, "burst_count": 1,
            "species": [], "bursts": [{"photo_ids": [1, 2, 3]}],
        }],
        "summary": {"total_photos": 3, "encounter_count": 1, "burst_count": 1, "review_count": 3},
    }
    page.route("**/api/pipeline/page-init*", lambda route: route.fulfill(json={
        "results": results, "workspace_overrides": {},
        "review_readiness": {"state": "ready", "total_photos": 3},
    }))
    page.route("**/api/pipeline/group/state", lambda route: route.fulfill(json={
        "photos": {str(pid): {"flag": "none", "has_species_keyword": False} for pid in range(1, 4)},
    }))
    previews = io.BytesIO()
    Image.new("RGB", (2560, 1707), "green").save(previews, "JPEG")
    originals = io.BytesIO()
    Image.new("RGB", (6000, 4000), "green").save(originals, "JPEG")
    image_requests = []

    def serve_image(route):
        image_requests.append(route.request.url)
        body = originals if "/original" in route.request.url else previews
        route.fulfill(body=body.getvalue(), content_type="image/jpeg")

    page.route("**/photos/*/preview?*", serve_image)
    page.route("**/photos/*/original*", serve_image)
    page.goto(f"{live_server['url']}/pipeline/review?enc=0&burst=0")
    page.wait_for_function("grmState.seeded && document.getElementById('grmLoupePhoto').naturalWidth === 2560")
    return image_requests


def test_group_review_loads_previews_and_keeps_original_inspection(live_server, page):
    requests = _open_review(page, live_server)
    expect(page.locator("#grmResSlider")).to_have_value("1")
    assert requests
    assert all("/preview?size=2560" in url for url in requests)
    expect(page.locator("#grmCandidates .grm-card")).to_have_count(3)

    # 1:1 inspection must load actual originals, not just magnify previews.
    page.keyboard.press("z")
    expect(page.locator("#grmResSlider")).to_have_value("3")
    page.wait_for_function("document.getElementById('grmLoupePhoto').naturalWidth === 6000")
    assert any("/original" in url for url in requests)

    page.locator("#grmResSlider").evaluate("el => { el.value = 1; el.dispatchEvent(new Event('input')); }")
    page.wait_for_function("document.getElementById('grmLoupePhoto').naturalWidth === 2560")


def test_group_review_coalesces_cursor_moves_using_latest_position(live_server, page):
    _open_review(page, live_server)
    result = page.evaluate("""async () => {
        const original = grmApplyCardTransforms;
        let draws = 0;
        grmApplyCardTransforms = () => { draws++; original(); };
        const loupe = document.getElementById('grmLoupeImg');
        const rect = loupe.getBoundingClientRect();
        for (let i = 1; i <= 80; i++) {
            loupe.dispatchEvent(new MouseEvent('mousemove', {
                clientX: rect.left + rect.width * i / 100,
                clientY: rect.top + rect.height * 0.4,
            }));
        }
        const beforeFrame = draws;
        await new Promise(requestAnimationFrame);
        grmApplyCardTransforms = original;
        return {beforeFrame, draws, x: _grmLastHoverX, y: _grmLastHoverY,
                zoomed: document.querySelectorAll('#grmOverlay .grm-card-img-box.zoomed').length};
    }""")
    assert result["beforeFrame"] == 0
    assert result["draws"] == 1
    assert result["x"] == pytest.approx(80, abs=0.5)
    assert result["y"] == pytest.approx(40, abs=0.5)
    assert result["zoomed"] == 3


@pytest.mark.parametrize("finish", ["grmLoupeReset()", "closeGroupReview()", "openGroupReview(0, 0)"])
def test_group_review_cancels_stale_cursor_frame(live_server, page, finish):
    _open_review(page, live_server)
    result = page.evaluate("""async finish => {
        const original = grmPositionCrosshair;
        let draws = 0;
        grmPositionCrosshair = () => { draws++; };
        const loupe = document.getElementById('grmLoupeImg');
        loupe.dispatchEvent(new MouseEvent('mousemove', {clientX: 200, clientY: 200}));
        const pending = _grmHoverFrame !== null;
        eval(finish);
        await new Promise(requestAnimationFrame);
        grmPositionCrosshair = original;
        return {pending, draws};
    }""", finish)
    assert result == {"pending": True, "draws": 0}
