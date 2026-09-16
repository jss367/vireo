"""Settings → Quick filters drives the filter bar's button row."""

from playwright.sync_api import expect


def _open_settings(page, live_server):
    page.goto(live_server["url"] + "/settings", timeout=10_000)
    expect(page.locator("body")).to_have_attribute(
        "data-settings-ready", "true", timeout=10_000
    )
    # The add-form fills in from /api/filters/fields.
    expect(page.locator("#cfgShortcutField option").first).to_be_attached(
        timeout=10_000
    )
    return page.locator("#settingsSaveStatus")


def _saved(page, status, action):
    with page.expect_response(
        lambda r: r.url.endswith("/api/config") and r.request.method == "POST"
    ):
        action()
    expect(status).to_have_attribute("data-state", "saved", timeout=10_000)


def _open_browse(page, live_server):
    page.goto(live_server["url"] + "/browse", timeout=10_000)
    page.wait_for_selector("#grid .grid-card", timeout=15_000)
    expect(page.locator(".vf-shortcuts [data-shortcut]").first).to_be_enabled(
        timeout=10_000
    )


def test_settings_lists_the_buttons_the_bar_renders(live_server, page):
    _open_settings(page, live_server)
    rows = page.locator("#cfgFilterShortcutsList [data-shortcut-row]")
    expect(rows).to_have_count(5)
    expect(page.locator('[data-shortcut-row="missing_species"] input')).to_have_value(
        "Missing species"
    )
    # The row states what the button will actually filter on, and that the
    # missing-tag pair combines with OR rather than AND.
    expect(page.locator('[data-shortcut-row="missing_species"]')).to_contain_text(
        "Missing species"
    )
    expect(page.locator('[data-shortcut-row="missing_species"]')).to_contain_text(
        "OR-grouped with"
    )


def test_removing_and_renaming_a_quick_filter_reaches_the_bar(live_server, page):
    status = _open_settings(page, live_server)
    _saved(page, status, lambda: page.click(
        '[data-shortcut-row="missing_location"] button[title="Remove this button"]'
    ))
    _saved(page, status, lambda: page.fill(
        '[data-shortcut-row="missing_species"] input', "No ID yet"
    ))

    _open_browse(page, live_server)
    expect(page.locator('.vf-shortcuts [data-field="has_location_keyword"]')).to_have_count(0)
    species = page.locator('.vf-shortcuts [data-field="has_species"]')
    expect(species).to_have_text("No ID yet")

    # The chip names the button that set it, not a paraphrase of its rule.
    species.click()
    expect(page.locator(".vf-chips")).to_contain_text("No ID yet")
    # Two of the five seeded photos carry a species keyword.
    expect(page.locator("#grid .grid-card")).to_have_count(3)


def test_added_quick_filter_appears_on_the_bar_and_filters(live_server, page):
    status = _open_settings(page, live_server)
    page.select_option("#cfgShortcutField", "rating")
    page.select_option("#cfgShortcutOp", ">=")
    page.select_option("#cfgShortcutValue select", "4")
    # The button text defaults to what the rule says, before any edit.
    expect(page.locator("#cfgShortcutLabel")).to_have_value("Rating is at least 4 stars")
    page.fill("#cfgShortcutLabel", "Keepers")
    _saved(page, status, lambda: page.click("text=+ Add quick filter"))
    expect(page.locator("#cfgFilterShortcutsList [data-shortcut-row]")).to_have_count(6)

    _open_browse(page, live_server)
    keepers = page.locator('.vf-shortcuts button', has_text="Keepers")
    expect(keepers).to_be_visible()
    keepers.click()
    # One seeded photo is rated 4; the rest are unrated.
    expect(page.locator("#grid .grid-card")).to_have_count(1)
    expect(page.locator(".vf-chips")).to_contain_text("Keepers")
    # Clicking again clears exactly the clause the button added.
    keepers.click()
    expect(page.locator("#grid .grid-card")).to_have_count(5)
    expect(page.locator(".vf-chips")).not_to_contain_text("Keepers")


def test_clearing_every_quick_filter_leaves_a_working_bar(live_server, page):
    status = _open_settings(page, live_server)
    for _ in range(5):
        _saved(page, status, lambda: page.locator(
            '#cfgFilterShortcutsList button[title="Remove this button"]'
        ).first.click())
    expect(page.locator("#cfgFilterShortcutsList")).to_contain_text("No quick filters")

    page.goto(live_server["url"] + "/browse", timeout=10_000)
    page.wait_for_selector("#grid .grid-card", timeout=15_000)
    expect(page.locator(".vf-shortcuts [data-shortcut]")).to_have_count(0)
    page.locator(".vf-search input").fill("hawk")
    page.wait_for_function(
        "document.querySelector('.vf-total strong').textContent === '3'",
        timeout=10_000,
    )

    # Restoring puts the built-ins back without touching anything else.
    status = _open_settings(page, live_server)
    _saved(page, status, lambda: page.click("text=Restore the default buttons"))
    expect(page.locator("#cfgFilterShortcutsList [data-shortcut-row]")).to_have_count(5)
