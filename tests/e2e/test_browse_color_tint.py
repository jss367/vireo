"""The browse grid tints a card with its color label.

A color label is a property of the photo, so it has to be visible on the card
without depending on the optional `color_label` badge field — which is off by
default, so a labelled photo used to look identical to an unlabelled one.
"""

from playwright.sync_api import expect


def test_card_carries_its_saved_color_label(live_server, page):
    """A label saved before the page loads reaches the card via the async fetch."""
    url = live_server["url"]
    photo_id = live_server["data"]["photos"][0]

    assert page.request.post(
        f"{url}/api/photos/{photo_id}/color_label", data={"color": "purple"}
    ).ok

    page.goto(f"{url}/browse")
    card = page.locator(f'.grid-card[data-id="{photo_id}"]')
    card.wait_for(state="visible")
    expect(card).to_have_attribute("data-color-label", "purple")

    # The tint is a real computed border color, not just an attribute.
    border = card.evaluate("el => getComputedStyle(el).borderTopColor")
    assert border not in ("", "rgba(0, 0, 0, 0)")

    # Unlabelled cards stay untinted.
    other = page.locator(f'.grid-card:not([data-id="{photo_id}"])').first
    assert other.get_attribute("data-color-label") is None


def test_setting_and_clearing_a_color_updates_the_card_live(live_server, page):
    """The detail panel's color buttons repaint the card without a reload."""
    url = live_server["url"]
    photo_id = live_server["data"]["photos"][0]

    page.goto(f"{url}/browse")
    card = page.locator(f'.grid-card[data-id="{photo_id}"]')
    card.wait_for(state="visible")
    card.click()

    green = page.locator('#detailColors [data-color="green"]')
    expect(green).to_be_visible()
    green.click()
    expect(card).to_have_attribute("data-color-label", "green")

    # Clicking the active color clears it, and the tint goes with it.
    green.click()
    expect(card).not_to_have_attribute("data-color-label", "green")
    assert card.get_attribute("data-color-label") is None
