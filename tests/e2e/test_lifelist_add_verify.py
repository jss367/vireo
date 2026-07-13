"""E2E verification for species representative surfaces (lightbox + browse menu).

Seed (from conftest.seed_e2e_data): photos[0]=hawk1 tagged "Red-tailed Hawk",
photos[3]=robin1 tagged "American Robin"; the other three carry predictions but
no accepted species keyword, so they are the zero-species cases.
"""
import re

from playwright.sync_api import expect


def test_browse_menu_sets_representative(live_server, page):
    url = live_server["url"]
    hawk = live_server["data"]["photos"][0]
    page.goto(f"{url}/browse")

    card = page.locator(f'.grid-card[data-id="{hawk}"]')
    card.wait_for(state="visible")
    card.click(button="right")

    menu = page.locator(".vireo-ctx-menu")
    expect(menu).to_be_visible()
    item = menu.locator(".vireo-ctx-item", has_text="Set Representative — Red-tailed Hawk")
    expect(item).to_be_visible()

    # The menu handler is fire-and-forget (safeFetch(...).then(...)); wait for
    # the POST to settle before reading state, or the /api/photos read races
    # the write and intermittently sees is_current_photo: false.
    with page.expect_response(
        lambda r: "/api/photo-preferences" in r.url and r.status == 200
    ):
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
    assert life_list == [{
        "species": "Red-tailed Hawk",
        "is_current_photo": True,
        "is_species_representative": True,
    }], life_list

    expect(card.locator(".representative-badge", has_text="Representative")).to_be_visible()
    card.click(button="right")
    item = page.locator(".vireo-ctx-menu .vireo-ctx-item", has_text="Set Representative — Red-tailed Hawk")
    expect(item).to_be_visible()
    assert "vireo-ctx-disabled" in (item.get_attribute("class") or "")
    assert item.get_attribute("title") == "Already representative"


def test_browse_menu_hidden_for_photo_without_species(live_server, page):
    url = live_server["url"]
    no_species = live_server["data"]["photos"][1]  # hawk2 — no accepted keyword
    page.goto(f"{url}/browse")

    card = page.locator(f'.grid-card[data-id="{no_species}"]')
    card.wait_for(state="visible")
    card.click(button="right")

    menu = page.locator(".vireo-ctx-menu")
    expect(menu).to_be_visible()
    expect(menu.locator(".vireo-ctx-item", has_text="Set Representative")).to_have_count(0)


def test_browse_menu_hidden_for_rejected_photo(live_server, page):
    """A species-tagged photo that has been rejected must not surface an
    "Set Representative" menu item — the server backstop
    (_photo_can_be_life_list_preference) rejects flag='rejected', so offering
    it in the menu would only ever produce an error toast."""
    url = live_server["url"]
    hawk = live_server["data"]["photos"][0]
    page.goto(f"{url}/browse")

    # Flag the species-tagged hawk photo as rejected via the same API the
    # browse UI uses, then reload so the local `photos` array reflects it.
    page.evaluate(
        """async (pid) => {
            await fetch('/api/batch/flag', {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({ photo_ids: [pid], flag: 'rejected' }),
            });
        }""",
        hawk,
    )
    page.reload()

    card = page.locator(f'.grid-card[data-id="{hawk}"]')
    card.wait_for(state="visible")
    card.click(button="right")

    menu = page.locator(".vireo-ctx-menu")
    expect(menu).to_be_visible()
    expect(menu.locator(".vireo-ctx-item", has_text="Set Representative")).to_have_count(0)


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
    item = menu.locator(".vireo-ctx-item", has_text="Set Representative")
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
    add_btn = panel.locator("button", has_text="Set Representative")
    expect(add_btn).to_be_visible()
    add_btn.click()

    selected = panel.locator("button.primary", has_text="Representative")
    expect(selected).to_be_visible()
    assert re.search(r"\bprimary\b", selected.get_attribute("class") or "")


def test_lightbox_panel_hides_after_current_photo_rejected(live_server, page):
    """When the currently-open photo is rejected via lightbox flag controls,
    _photo_can_be_life_list_preference stops accepting it on the server. The
    panel must refresh so "Set Representative" stops offering clicks that would
    be guaranteed 4xxs."""
    url = live_server["url"]
    page.goto(f"{url}/life-list")

    card = page.locator(".species-card").first
    card.wait_for(state="visible")
    card.click()

    panel = page.locator("#lifeListLightboxPanel")
    expect(panel).to_be_visible()
    expect(panel.locator("button", has_text="Set Representative")).to_be_visible()

    # Reject via the same code path the lightbox flag chips call.
    page.evaluate("() => _lbApplyFlag(_lightboxCurrentId, 'rejected')")

    # After the flag write settles and the listener refetches, the panel
    # should hide (backend returns empty life_list for rejected photos).
    expect(panel).to_be_hidden()


def test_sort_and_numbering_preferences_persist(live_server, page):
    url = live_server["url"]
    page.goto(f"{url}/life-list")
    page.locator(".species-card").first.wait_for(state="visible")

    page.locator("#sortSelect").select_option("alpha")
    expect(page.locator(".species-name").first).to_have_text("American Robin")

    # Fixed numbering preserves the chronological lifer number even though
    # alphabetical order puts the newer robin first.
    expect(page.locator(".lifer-number").first).to_have_text("#2")
    page.locator("#renumberView").check()
    expect(page.locator(".lifer-number").first).to_have_text("#1")

    page.reload()
    page.locator(".species-card").first.wait_for(state="visible")
    expect(page.locator("#sortSelect")).to_have_value("alpha")
    expect(page.locator("#renumberView")).to_be_checked()
    expect(page.locator(".species-name").first).to_have_text("American Robin")
    expect(page.locator(".lifer-number").first).to_have_text("#1")
