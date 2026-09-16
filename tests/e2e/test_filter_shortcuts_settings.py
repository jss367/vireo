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


def test_renamed_flag_button_names_its_own_chip(live_server, page):
    """One active enum value is one button, so the chip uses its words."""
    status = _open_settings(page, live_server)
    _saved(page, status, lambda: page.fill(
        '[data-shortcut-row="flag_picked"] input', "Portfolio"
    ))

    _open_browse(page, live_server)
    page.locator('.vf-shortcuts [data-value="flagged"]').click()
    expect(page.locator(".vf-chips")).to_contain_text("Portfolio")
    # A second value has to name both, so the chip states the full clause.
    page.locator('.vf-shortcuts [data-value="none"]').click()
    expect(page.locator(".vf-chips")).to_contain_text("Flag is one of Picked, Unflagged")


def test_internal_only_fields_are_not_offered_as_quick_filters(live_server, page):
    """`life_list_uncounted` takes an encoded token, not typed text."""
    _open_settings(page, live_server)
    options = page.locator("#cfgShortcutField option").all_inner_texts()
    assert "Uncounted identification" not in options
    assert "Rating" in options


def test_the_cap_is_visible_and_enforced_before_a_row_is_lost(live_server, page):
    """The server refuses a longer list, so the form must stop first."""
    url = live_server["url"]
    filled = [
        {"id": f"s{i}", "label": f"S{i}",
         "rules": {"field": "rating", "op": ">=", "value": 4}}
        for i in range(24)
    ]
    page.request.post(f"{url}/api/config", data={"filter_shortcuts": filled},
                      headers={"Content-Type": "application/json"})
    _open_settings(page, live_server)
    expect(page.locator("#cfgShortcutCount")).to_have_text("24 of 24 quick filters")

    page.select_option("#cfgShortcutField", "rating")
    page.click("text=+ Add quick filter")
    expect(page.locator("#cfgFilterShortcutsList [data-shortcut-row]")).to_have_count(24)
    expect(page.locator("#toastContainer > *").first).to_contain_text("remove one first")


def test_button_text_stops_where_the_stored_label_does(live_server, page):
    """Normalization truncates at 40, so the form must not show more."""
    status = _open_settings(page, live_server)
    long_text = "N" * 60
    row_input = page.locator('[data-shortcut-row="missing_species"] input')
    _saved(page, status, lambda: row_input.fill(long_text))
    assert len(row_input.input_value()) == 40

    page.fill("#cfgShortcutLabel", long_text)
    assert len(page.locator("#cfgShortcutLabel").input_value()) == 40

    # What the bar renders matches what Settings showed.
    _open_browse(page, live_server)
    expect(page.locator('.vf-shortcuts [data-field="has_species"]')).to_have_text("N" * 40)


def test_clearing_a_label_shows_the_text_that_will_be_stored(live_server, page):
    """An empty label is replaced on save, so Settings must not stay blank."""
    status = _open_settings(page, live_server)
    row_input = page.locator('[data-shortcut-row="missing_species"] input')
    row_input.fill("")
    _saved(page, status, lambda: page.locator("#cfgShortcutLabel").click())
    expect(row_input).to_have_value("Missing species")

    _open_browse(page, live_server)
    expect(page.locator('.vf-shortcuts [data-field="has_species"]')).to_have_text(
        "Missing species"
    )


def test_removing_one_of_a_pair_keeps_the_other_in_control(live_server, page):
    """A persisted OR clause outlives the button that helped build it."""
    _open_browse(page, live_server)
    page.locator('.vf-shortcuts [data-field="has_species"]').click()
    page.locator('.vf-shortcuts [data-field="has_location_keyword"]').click()
    expect(page.locator(".vf-chips")).to_contain_text(
        "Missing species OR Missing location tag"
    )
    # The bar persists the expression on an 800 ms debounce; navigating
    # before it lands would leave nothing to restore.
    page.wait_for_timeout(1500)

    status = _open_settings(page, live_server)
    _saved(page, status, lambda: page.click(
        '[data-shortcut-row="missing_location"] button[title="Remove this button"]'
    ))

    _open_browse(page, live_server)
    species = page.locator('.vf-shortcuts [data-field="has_species"]')
    # The persisted clause still contains this field, so the button is on.
    expect(species).to_have_attribute("aria-pressed", "true")
    species.click()
    # Its field leaves the clause; the orphaned one stays, and nothing is
    # duplicated beside it.
    expect(species).to_have_attribute("aria-pressed", "false")
    expect(page.locator(".vf-chips")).to_contain_text("Missing location tag")
    expect(page.locator(".vf-chips")).not_to_contain_text("Missing species")
    assert page.evaluate("VireoFilter.getUserRules()")["rules"].__len__() == 1


def test_a_shortcut_narrows_an_exclusion_instead_of_replacing_it(live_server, page):
    """A quick filter must not delete a rule built in the popover."""
    _open_browse(page, live_server)
    page.evaluate(
        "VireoFilter.loadExpression({mode: 'all', rules: ["
        "  {field: 'flag', op: 'not_in', value: ['rejected']}]})"
    )
    expect(page.locator(".vf-chips")).to_contain_text("Flag is not one of Rejected")

    page.locator('.vf-shortcuts [data-value="flagged"]').click()
    rules = page.evaluate("VireoFilter.getUserRules()")["rules"]
    assert {"field": "flag", "op": "not_in", "value": ["rejected"]} in rules
    assert {"field": "flag", "op": "in", "value": ["flagged"]} in rules
    # And toggling back off leaves the exclusion alone.
    page.locator('.vf-shortcuts [data-value="flagged"]').click()
    rules = page.evaluate("VireoFilter.getUserRules()")["rules"]
    assert rules == [{"field": "flag", "op": "not_in", "value": ["rejected"]}]


def test_shortcut_finds_its_clause_behind_an_exclusion(live_server, page):
    """Order must not decide whether a button reads as on."""
    _open_browse(page, live_server)
    page.evaluate(
        "VireoFilter.loadExpression({mode: 'all', rules: ["
        "  {field: 'flag', op: 'not_in', value: ['rejected']},"
        "  {field: 'flag', op: 'in', value: ['flagged']}]})"
    )
    picked = page.locator('.vf-shortcuts [data-value="flagged"]')
    expect(picked).to_have_attribute("aria-pressed", "true")

    # Clicking clears its own value out of that clause — no duplicate beside it.
    picked.click()
    rules = page.evaluate("VireoFilter.getUserRules()")["rules"]
    assert rules == [{"field": "flag", "op": "not_in", "value": ["rejected"]}]
    expect(picked).to_have_attribute("aria-pressed", "false")


def test_toggling_off_clears_a_duplicated_clause(live_server, page):
    """One click means off, even if the expression held the clause twice."""
    url = live_server["url"]
    page.request.post(f"{url}/api/config", data={"filter_shortcuts": [
        {"id": "keepers", "label": "Keepers",
         "rules": {"field": "rating", "op": ">=", "value": 4}},
    ]}, headers={"Content-Type": "application/json"})
    _open_browse(page, live_server)
    page.evaluate(
        "VireoFilter.loadExpression({mode: 'all', rules: ["
        "  {field: 'rating', op: '>=', value: 4},"
        "  {field: 'rating', op: '>=', value: 4}]})"
    )
    keepers = page.locator('.vf-shortcuts button', has_text="Keepers")
    expect(keepers).to_have_attribute("aria-pressed", "true")

    keepers.click()
    expect(keepers).to_have_attribute("aria-pressed", "false")
    assert page.evaluate("VireoFilter.getUserRules()") == {"mode": "all", "rules": []}


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
