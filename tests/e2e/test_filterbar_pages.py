"""E2E: universal filter bar on Map, Review, Duplicates, and Misses,
plus the cross-page "Open results in…" handoff.

Seed data (conftest): 3 hawk photos in /photos/park, 2 robins in
/photos/yard; hawk1 rated 4; species keywords on hawk1/robin1; every
photo has a pending prediction.
"""


def _wait_bar(page):
    page.wait_for_selector("#vireoFilterBar", timeout=15000)
    page.wait_for_function(
        "document.querySelector('.vf-total strong') && "
        "document.querySelector('.vf-total strong').textContent !== '–'",
        timeout=15000,
    )


def _total(page):
    return int(page.inner_text(".vf-total strong").replace(",", ""))


def test_map_page_uses_filter_bar(live_server, page):
    db = live_server["db"]
    photos = {p["filename"]: p["id"] for p in db.get_photos()}
    with db.conn:
        db.conn.execute(
            "UPDATE photos SET latitude=37.7, longitude=-122.4 WHERE id IN (?, ?)",
            (photos["hawk1.jpg"], photos["robin1.jpg"]))

    page.goto(live_server["url"] + "/map")
    _wait_bar(page)
    assert _total(page) == 2
    assert "Plottable locations" in page.inner_text(".vf-scope-chip")

    search = page.locator(".vf-search input")
    search.fill("hawk")
    search.press("Enter")
    page.wait_for_function(
        "document.querySelector('.vf-total strong').textContent === '1'",
        timeout=8000,
    )
    # Sidebar reflects the filtered plottable set.
    page.wait_for_function(
        "document.querySelectorAll('.map-sidebar .sidebar-item, .map-sidebar li').length <= 1",
        timeout=8000,
    )


def test_review_page_uses_filter_bar(live_server, page):
    page.goto(live_server["url"] + "/review")
    _wait_bar(page)
    assert "Review · Predictions" in page.inner_text(".vf-scope-chip")
    page.wait_for_selector(".pred-card, .grid .card, #grid > *", timeout=15000)
    before = page.evaluate("predictions.length")
    assert before == 5

    search = page.locator(".vf-search input")
    search.fill("robin")
    search.press("Enter")
    page.wait_for_function("predictions.length === 2", timeout=8000)
    chips = page.evaluate("document.querySelector('.vf-chips').textContent")
    assert "robin" in chips

    # Review-only fields appear in this page's picker.
    page.click(".vf-filters-btn")
    page.click(".vf-add-filter")
    page.fill(".vf-field-search", "prediction status")
    page.wait_for_selector('[data-add-field="prediction_status"]', timeout=8000)


def test_misses_page_uses_filter_bar(live_server, page):
    db = live_server["db"]
    photo_ids = live_server["data"]["photos"]
    with db.conn:
        db.conn.executemany(
            "UPDATE photos SET miss_no_subject=1, miss_computed_at='2026-04-22' "
            "WHERE id=?",
            [(photo_id,) for photo_id in photo_ids],
        )

    page.goto(live_server["url"] + "/misses")
    _wait_bar(page)
    assert "Misses · Detected misses" in page.inner_text(".vf-scope-chip")
    assert _total(page) == 5

    search = page.locator(".vf-search input")
    search.fill("robin")
    search.press("Enter")
    page.wait_for_function(
        "document.querySelector('.vf-total strong').textContent === '2'",
        timeout=8000,
    )
    assert page.locator("[data-testid^='miss-card-no_subject-']").count() == 2

    page.click(".vf-handoff")
    page.wait_for_selector('.vf-handoff-menu [data-handoff-path="/browse"]')


def test_misses_page_hides_rejected_flag_filter(live_server, page):
    page.goto(live_server["url"] + "/misses")
    _wait_bar(page)

    page.click(".vf-filters-btn")
    assert page.locator('.vf-quick-flags [data-flag="rejected"]').is_hidden()

    page.click(".vf-add-filter")
    page.fill(".vf-field-search", "flag")
    page.click('[data-add-field="flag"]')
    assert page.locator(
        '.vf-enum-pill[data-value="rejected"]'
    ).count() == 0


def test_browse_picker_hides_review_only_fields(live_server, page):
    page.goto(live_server["url"] + "/browse")
    _wait_bar(page)
    page.click(".vf-filters-btn")
    page.click(".vf-add-filter")
    page.fill(".vf-field-search", "prediction")
    page.wait_for_timeout(400)
    assert page.locator('[data-add-field="prediction_status"]').count() == 0


def test_duplicates_page_filters_members(live_server, page):
    db = live_server["db"]
    photos = {p["filename"]: p["id"] for p in db.get_photos()}
    with db.conn:
        db.conn.execute(
            "UPDATE photos SET file_hash='dupehash' WHERE id IN (?, ?)",
            (photos["hawk1.jpg"], photos["hawk2.jpg"]))

    page.goto(live_server["url"] + "/duplicates")
    page.wait_for_selector("#vireoFilterBar", timeout=15000)
    page.click("#scanBtn")
    page.wait_for_selector(".dup-group", timeout=20000)

    # Filter to a query only hawk1 matches: the group stays visible (a
    # matching member reveals its complete group) and hawk1 is badged.
    search = page.locator(".vf-search input")
    search.fill("hawk1")
    search.press("Enter")
    page.wait_for_selector(".filter-match-badge", timeout=8000)
    assert page.locator(".dup-group").count() >= 1
    assert page.locator(".filter-match-badge").count() == 1

    # A filter matching no member hides the group with an explanation.
    search.fill("robin")
    search.press("Enter")
    page.wait_for_function(
        "document.getElementById('results').textContent.includes('No duplicate groups match')",
        timeout=8000,
    )

    # Clearing restores everything.
    page.click(".vf-clear")
    page.wait_for_selector(".dup-group", timeout=8000)


def test_handoff_carries_expression_to_map(live_server, page):
    db = live_server["db"]
    photos = {p["filename"]: p["id"] for p in db.get_photos()}
    with db.conn:
        db.conn.execute(
            "UPDATE photos SET latitude=37.7, longitude=-122.4 WHERE id = ?",
            (photos["hawk1.jpg"],))

    page.goto(live_server["url"] + "/browse")
    _wait_bar(page)
    search = page.locator(".vf-search input")
    search.fill("hawk")
    search.press("Enter")
    page.wait_for_function(
        "document.querySelector('.vf-total strong').textContent === '3'",
        timeout=8000,
    )

    page.click(".vf-handoff")
    page.wait_for_selector(".vf-handoff-menu button", timeout=8000)
    page.click('.vf-handoff-menu [data-handoff-path="/map"]')
    page.wait_for_url("**/map?filters=*", timeout=15000)
    _wait_bar(page)
    # Expression transferred; Map adds its own scope: only hawk1 is plottable.
    chips = page.evaluate("document.querySelector('.vf-chips').textContent")
    assert "hawk" in chips
    assert "Plottable locations" in page.inner_text(".vf-scope-chip")
    assert _total(page) == 1
