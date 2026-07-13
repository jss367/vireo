import base64

from playwright.sync_api import expect

PNG_1X1 = base64.b64decode(
    "iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAQAAAC1HAwCAAAAC0lEQVR42mNk+A8AAQUBAScY42YAAAAASUVORK5CYII="
)


def test_browse_compare_two_selected_photos(live_server, page):
    url = live_server["url"]
    page.goto(f"{url}/browse")

    cards = page.locator(".grid-card")
    cards.first.wait_for(state="visible")
    assert cards.count() >= 2

    first_name = cards.nth(0).get_attribute("data-filename")
    second_name = cards.nth(1).get_attribute("data-filename")

    cards.nth(0).click(modifiers=["Meta"])
    cards.nth(1).click(modifiers=["Meta"])

    compare_btn = page.locator("#compareBtn")
    expect(compare_btn).to_be_visible()
    compare_btn.click()

    overlay = page.locator("#browseCompareOverlay")
    expect(overlay).to_have_class("browse-compare-overlay active")
    expect(page.locator("#browseCompareNameA")).to_have_text(first_name)
    expect(page.locator("#browseCompareNameB")).to_have_text(second_name)
    expect(page.locator("#browseCompareCount")).to_have_text("1-2 of 2")

    page.keyboard.press("Escape")
    expect(overlay).not_to_have_class("browse-compare-overlay active")


def test_browse_compare_steps_through_selected_pairs(live_server, page):
    url = live_server["url"]
    page.goto(f"{url}/browse")

    cards = page.locator(".grid-card")
    cards.first.wait_for(state="visible")
    assert cards.count() >= 3

    second_name = cards.nth(1).get_attribute("data-filename")
    third_name = cards.nth(2).get_attribute("data-filename")

    cards.nth(0).click(modifiers=["Meta"])
    cards.nth(1).click(modifiers=["Meta"])
    cards.nth(2).click(modifiers=["Meta"])
    page.locator("#compareBtn").click()

    page.keyboard.press("ArrowRight")

    expect(page.locator("#browseCompareNameA")).to_have_text(second_name)
    expect(page.locator("#browseCompareNameB")).to_have_text(third_name)
    expect(page.locator("#browseCompareCount")).to_have_text("2-3 of 3")


def test_browse_compare_opens_with_c_shortcut(live_server, page):
    url = live_server["url"]
    page.goto(f"{url}/browse")

    cards = page.locator(".grid-card")
    cards.first.wait_for(state="visible")
    assert cards.count() >= 2

    cards.nth(0).click(modifiers=["Meta"])
    cards.nth(1).click(modifiers=["Meta"])
    page.keyboard.press("c")

    expect(page.locator("#browseCompareOverlay")).to_have_class(
        "browse-compare-overlay active"
    )


def test_browse_compare_zoom_is_independent_and_can_be_reset(live_server, page):
    url = live_server["url"]
    page.route(
        "**/photos/*/original",
        lambda route: route.fulfill(status=200, content_type="image/png", body=PNG_1X1),
    )
    page.goto(f"{url}/browse")

    cards = page.locator(".grid-card")
    cards.first.wait_for(state="visible")
    assert cards.count() >= 2

    cards.nth(0).click(modifiers=["Meta"])
    cards.nth(1).click(modifiers=["Meta"])
    page.locator("#compareBtn").click()

    left = page.locator("#browseCompareWrapA")
    left.dispatch_event(
        "wheel", {"deltaY": -300, "clientX": 250, "clientY": 250}
    )

    expect(left).to_have_class("browse-compare-image-wrap zoomed")
    expect(page.locator("#browseCompareWrapB")).to_have_class(
        "browse-compare-image-wrap"
    )
    expect(page.locator("#browseCompareZoomA")).not_to_have_text("Fit")
    expect(page.locator("#browseCompareZoomB")).to_have_text("Fit")
    expect(page.locator("#browseCompareImgA")).to_have_attribute(
        "data-original-loaded", "true"
    )
    assert page.locator("#browseCompareImgA").get_attribute("src").endswith(
        "/original"
    )

    page.get_by_role("button", name="Reset views").click()
    expect(left).to_have_class("browse-compare-image-wrap")
    expect(page.locator("#browseCompareZoomA")).to_have_text("Fit")


def test_browse_compare_double_click_toggles_detail_zoom(live_server, page):
    url = live_server["url"]
    page.goto(f"{url}/browse")

    cards = page.locator(".grid-card")
    cards.first.wait_for(state="visible")
    assert cards.count() >= 2

    cards.nth(0).click(modifiers=["Meta"])
    cards.nth(1).click(modifiers=["Meta"])
    page.locator("#compareBtn").click()

    left = page.locator("#browseCompareWrapA")
    left.dblclick(position={"x": 200, "y": 200})
    expect(page.locator("#browseCompareZoomA")).to_have_text("200%")

    left.dblclick(position={"x": 200, "y": 200})
    expect(page.locator("#browseCompareZoomA")).to_have_text("Fit")


def test_browse_compare_keeps_preview_when_original_fails(live_server, page):
    url = live_server["url"]
    page.route(
        "**/photos/*/full",
        lambda route: route.fulfill(status=200, content_type="image/png", body=PNG_1X1),
    )
    page.route("**/photos/*/original", lambda route: route.fulfill(status=404))
    page.goto(f"{url}/browse")

    cards = page.locator(".grid-card")
    cards.first.wait_for(state="visible")
    assert cards.count() >= 2

    cards.nth(0).click(modifiers=["Meta"])
    cards.nth(1).click(modifiers=["Meta"])
    page.locator("#compareBtn").click()

    image = page.locator("#browseCompareImgA")
    page.locator("#browseCompareWrapA").dblclick(position={"x": 200, "y": 200})

    expect(image).to_have_attribute("data-original-loaded", "failed")
    assert image.get_attribute("src").endswith("/full")
    assert image.evaluate("img => img.naturalWidth") > 0
