from playwright.sync_api import expect


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
