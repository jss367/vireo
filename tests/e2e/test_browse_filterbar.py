"""E2E: universal filter bar on Browse (Phase 2).

Ports the interaction checks from the design prototype's verify suite
(docs/plans/2026-07-19-photo-filter-prototype/verify_features.py) against
the real page: chip semantics, quick-filter multi-select, single
replaceable quick-search clause, pause/resume, typeahead counts,
persistence, and select-all consistency with the filtered grid.

Seed data (conftest): 3 hawk photos in /photos/park, 2 robins in
/photos/yard; hawk1 has rating 4 and the Red-tailed Hawk species keyword.
"""


def _total(page):
    return int(page.inner_text(".vf-total strong").replace(",", ""))


def _wait_total(page, expected, timeout=8000):
    page.wait_for_function(
        "expected => document.querySelector('.vf-total strong')"
        f".textContent.replace(/,/g, '') === String({expected})",
        timeout=timeout,
    )


def _open_browse(page, live_server):
    page.goto(live_server["url"] + "/browse")
    page.wait_for_selector("#grid .grid-card", timeout=15000)
    page.wait_for_selector("#vireoFilterBar", timeout=15000)
    page.wait_for_function(
        "document.querySelector('.vf-total strong').textContent !== '–'",
        timeout=15000,
    )


def test_quick_rating_filter_and_chip_semantics(live_server, page):
    _open_browse(page, live_server)
    assert _total(page) == 5

    page.click(".vf-filters-btn")
    page.click('.vf-quick-rating .vf-star[data-rating="4"]')
    _wait_total(page, 1)
    chips = page.evaluate("document.querySelector('.vf-chips').textContent")
    assert "Rating is at least 4 stars" in chips
    # Toggling the same star clears the rule.
    page.click('.vf-quick-rating .vf-star[data-rating="4"]')
    _wait_total(page, 5)


def test_quick_flags_multi_select_combines(live_server, page):
    _open_browse(page, live_server)
    page.click(".vf-filters-btn")
    page.click('.vf-quick-flags [data-flag="flagged"]')
    page.wait_for_timeout(300)
    page.click('.vf-quick-flags [data-flag="none"]')
    # Seed photos have NULL flags — all 5 must count as Unflagged.
    _wait_total(page, 5)
    chips = page.evaluate("document.querySelector('.vf-chips').textContent")
    assert "Flag is one of Picked, Unflagged" in chips


def test_quick_search_is_single_replaceable_clause(live_server, page):
    _open_browse(page, live_server)
    search = page.locator(".vf-search input")
    search.fill("hawk")
    search.press("Enter")
    _wait_total(page, 3)
    search.fill("robin")
    search.press("Enter")
    _wait_total(page, 2)
    chips = page.evaluate("document.querySelector('.vf-chips').textContent")
    assert "robin" in chips and "hawk" not in chips


def test_pause_resume_with_backslash(live_server, page):
    _open_browse(page, live_server)
    page.click(".vf-filters-btn")
    page.click('.vf-quick-rating .vf-star[data-rating="4"]')
    _wait_total(page, 1)
    page.click(".vf-done")

    page.keyboard.press("\\")
    _wait_total(page, 5)
    note = page.inner_text(".vf-paused-note")
    assert "Filters paused" in note
    page.wait_for_function(
        "document.querySelector('.vf-paused-note').textContent.includes('1 would match')",
        timeout=8000,
    )
    chips_row_class = page.get_attribute(".vf-chip-row", "class")
    assert "muted" in chips_row_class

    page.keyboard.press("\\")
    _wait_total(page, 1)
    assert page.locator(".vf-paused-note").is_hidden()


def test_rule_builder_typeahead_counts_and_pick(live_server, page):
    _open_browse(page, live_server)
    page.click(".vf-filters-btn")
    page.click(".vf-add-filter")
    page.fill(".vf-field-search", "species")
    page.click('[data-add-field="species"]')
    page.wait_for_timeout(400)
    value_input = page.locator('.vf-rule-tree [data-suggest="1"]')
    value_input.click()
    page.wait_for_selector(".vf-suggest .vf-value-option", timeout=8000)
    options = page.locator(".vf-suggest .vf-value-option")
    texts = [options.nth(i).inner_text() for i in range(options.count())]
    assert any("Red-tailed Hawk" in t for t in texts)
    assert all(any(ch.isdigit() for ch in t) for t in texts), texts
    # Pick the hawk option (typeahead narrows first).
    value_input.type("red", delay=30)
    page.wait_for_timeout(600)
    page.locator(".vf-suggest .vf-value-option").first.click()
    _wait_total(page, 1)
    chips = page.evaluate("document.querySelector('.vf-chips').textContent")
    assert "Species contains Red-tailed Hawk" in chips


def test_filter_state_persists_across_reload(live_server, page):
    _open_browse(page, live_server)
    search = page.locator(".vf-search input")
    search.fill("hawk")
    search.press("Enter")
    _wait_total(page, 3)
    page.wait_for_timeout(1200)  # persist debounce

    page.reload()
    page.wait_for_selector("#vireoFilterBar", timeout=15000)
    _wait_total(page, 3, timeout=15000)
    chips = page.evaluate("document.querySelector('.vf-chips').textContent")
    assert "hawk" in chips


def test_select_all_matches_filtered_grid(live_server, page):
    """Select-all must resolve exactly the photos the filtered grid shows
    (hard requirement: no surface may disagree with the visible result)."""
    _open_browse(page, live_server)
    search = page.locator(".vf-search input")
    search.fill("hawk")
    search.press("Enter")
    _wait_total(page, 3)
    search.press("Escape")  # blur: select-all is a grid shortcut, not an input one
    page.keyboard.press("ControlOrMeta+a")
    page.wait_for_function(
        "window.selectedPhotos && selectedPhotos.size === 3", timeout=8000,
    )


def test_visual_search_error_state_is_honest(live_server, page):
    """A visual clause that cannot run (no embeddings indexed) must show an
    error chip + explanation and keep applying metadata filters — never
    silently return zero results (Phase 3 hard requirement)."""
    _open_browse(page, live_server)
    search = page.locator(".vf-search input")
    search.fill("an owl at dusk")
    page.wait_for_selector(".vf-search-suggest button", timeout=8000)
    page.locator('.vf-search-suggest [data-search-kind="visual"]').click()

    # Visual chip appears in an error state, results stay metadata-only (5).
    page.wait_for_selector(".vf-chip.visual", timeout=8000)
    _wait_total(page, 5)
    page.wait_for_function(
        "document.querySelector('.vf-visual-note') && "
        "!document.querySelector('.vf-visual-note').hidden",
        timeout=8000,
    )
    note = page.inner_text(".vf-visual-note")
    assert "metadata filters shown only" in note
    assert page.locator(".vf-chip.visual.error").count() == 1

    # Metadata rules still apply alongside the broken visual clause.
    page.click(".vf-filters-btn")
    page.click('.vf-quick-rating .vf-star[data-rating="4"]')
    _wait_total(page, 1)
    page.click(".vf-done")

    # The clause persists across reload and stays honestly marked.
    page.wait_for_timeout(1200)
    page.reload()
    page.wait_for_selector("#vireoFilterBar", timeout=15000)
    _wait_total(page, 1, timeout=15000)
    page.wait_for_selector(".vf-chip.visual", timeout=8000)
    chips = page.evaluate("document.querySelector('.vf-chips').textContent")
    assert "Visually similar" in chips and "an owl at dusk" in chips

    # Removing the visual chip keeps the metadata filter.
    page.evaluate("document.querySelector('.vf-chip.visual .vf-chip-x').click()")
    page.wait_for_function(
        "!document.querySelector('.vf-chip.visual')", timeout=8000,
    )
    _wait_total(page, 1)


def test_visual_strength_control_and_popover_row(live_server, page):
    _open_browse(page, live_server)
    page.evaluate("VireoFilter.visualSearch('a hawk in flight')")
    page.wait_for_selector(".vf-chip.visual", timeout=8000)
    page.click(".vf-filters-btn")
    row = page.locator(".vf-visual-row")
    assert row.is_visible()
    assert "a hawk in flight" in row.inner_text()
    page.click('.vf-visual-strength [data-strength="strict"]')
    page.wait_for_timeout(400)
    assert page.evaluate("VireoFilter.getVisual().strength") == "strict"
    page.click('.vf-visual-row [data-action="visual-remove"]')
    page.wait_for_function("!VireoFilter.getVisual()", timeout=8000)
