"""E2E verification for the "Add to Life List" surfaces (lightbox + browse menu).

Seed (from conftest.seed_e2e_data): photos[0]=hawk1 tagged "Red-tailed Hawk",
photos[3]=robin1 tagged "American Robin"; the other three carry predictions but
no accepted species keyword, so they are the zero-species cases.
"""
import re

from playwright.sync_api import expect


def test_browse_menu_add_to_life_list_sets_representative(live_server, page):
    url = live_server["url"]
    hawk = live_server["data"]["photos"][0]
    page.goto(f"{url}/browse")

    card = page.locator(f'.grid-card[data-id="{hawk}"]')
    card.wait_for(state="visible")
    card.click(button="right")

    menu = page.locator(".vireo-ctx-menu")
    expect(menu).to_be_visible()
    item = menu.locator(".vireo-ctx-item", has_text="Add to Life List — Red-tailed Hawk")
    expect(item).to_be_visible()
    item.click()

    # End-to-end: the click hit the real API and set THIS photo as the rep.
    life_list = page.evaluate(
        """async (pid) => {
            const r = await fetch('/api/photos/' + pid);
            const d = await r.json();
            return d.life_list;
        }""",
        hawk,
    )
    assert life_list == [{"species": "Red-tailed Hawk", "is_current_photo": True}], life_list


def test_browse_menu_hidden_for_photo_without_species(live_server, page):
    url = live_server["url"]
    no_species = live_server["data"]["photos"][1]  # hawk2 — no accepted keyword
    page.goto(f"{url}/browse")

    card = page.locator(f'.grid-card[data-id="{no_species}"]')
    card.wait_for(state="visible")
    card.click(button="right")

    menu = page.locator(".vireo-ctx-menu")
    expect(menu).to_be_visible()
    expect(menu.locator(".vireo-ctx-item", has_text="Add to Life List")).to_have_count(0)


def test_browse_menu_disabled_for_multi_select(live_server, page):
    url = live_server["url"]
    hawk = live_server["data"]["photos"][0]
    other = live_server["data"]["photos"][1]
    page.goto(f"{url}/browse")

    hawk_card = page.locator(f'.grid-card[data-id="{hawk}"]')
    hawk_card.wait_for(state="visible")
    hawk_card.click(modifiers=["Meta"])
    page.locator(f'.grid-card[data-id="{other}"]').click(modifiers=["Meta"])
    assert page.evaluate("selectedPhotos.size") == 2

    hawk_card.click(button="right")
    menu = page.locator(".vireo-ctx-menu")
    expect(menu).to_be_visible()
    item = menu.locator(".vireo-ctx-item", has_text="Add to Life List")
    expect(item).to_be_visible()
    assert "vireo-ctx-disabled" in (item.get_attribute("class") or "")
    assert item.get_attribute("title") == "Select a single photo"


def test_lightbox_panel_add_and_flip_to_selected(live_server, page):
    url = live_server["url"]
    page.goto(f"{url}/life-list")

    card = page.locator(".species-card").first
    card.wait_for(state="visible")
    card.click()

    panel = page.locator("#lifeListLightboxPanel")
    expect(panel).to_be_visible()
    add_btn = panel.locator("button", has_text="Add to Life List")
    expect(add_btn).to_be_visible()
    add_btn.click()

    selected = panel.locator("button", has_text="Life List photo")
    expect(selected).to_be_visible()
    assert re.search(r"\bprimary\b", selected.get_attribute("class") or "")
