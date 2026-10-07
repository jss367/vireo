import json

from playwright.sync_api import expect


def open_browse(page, live_server):
    page.goto(live_server["url"] + "/browse")
    expect(page.locator(".vf-total strong")).to_have_text("5", timeout=15000)
    return page.locator(".vf-search input")


def test_boolean_search_persists_and_composes_with_filters(live_server, page):
    search = open_browse(page, live_server)
    with page.expect_response(lambda r: r.request.method == 'PUT' and '/api/workspaces/' in r.url):
        search.fill("(hawk OR robin) NOT hawk2")
        expect(page.locator(".vf-total strong")).to_have_text("4")
    page.reload()
    expect(page.locator(".vf-search input")).to_have_value("(hawk OR robin) NOT hawk2")
    expect(page.locator(".vf-total strong")).to_have_text("4")
    # Narrow the parsed expression using the existing rule builder API.
    page.locator('.vf-filters-btn').click()
    page.locator('.vf-quick-rating [data-rating="4"]').click()
    expect(page.locator(".vf-total strong")).to_have_text("1")
    rules = page.evaluate("VireoFilter.getRules()")
    response = page.request.post(live_server["url"] + "/api/photos/query", data={
        "rules": rules, "ids_only": True,
    })
    assert response.ok
    assert len(response.json()["ids"]) == 1


def test_invalid_live_search_preserves_results_and_recovers(live_server, page):
    search = open_browse(page, live_server)
    search.fill("hawk")
    expect(page.locator(".vf-total strong")).to_have_text("3")
    applied = page.evaluate("VireoFilter.getRules()")
    search.fill("hawk OR (")
    expect(search).to_have_attribute("aria-invalid", "true")
    expect(page.locator(".vf-search-error")).to_contain_text("Showing the last applied filters")
    assert page.evaluate("VireoFilter.getRules()") == applied
    search.press("Enter")
    search.press("Tab")
    expect(search).to_have_value("hawk OR (")
    expect(page.locator(".vf-total strong")).to_have_text("3")
    search.fill('hawk OR "American Robin"')
    expect(search).to_have_attribute("aria-invalid", "false")
    expect(page.locator(".vf-search-error")).to_be_hidden()
    expect(page.locator(".vf-total strong")).to_have_text("5")

    search.fill("hawk")
    expect(page.locator(".vf-total strong")).to_have_text("3")
    search.fill("hawk OR")
    expect(search).to_have_attribute("aria-invalid", "true")
    page.locator('.vf-chip-row [data-chip-x]').click()
    expect(search).to_have_value("")
    expect(page.locator(".vf-search-error")).to_be_hidden()
    expect(page.locator(".vf-total strong")).to_have_text("5")
    search.fill('"unfinished')
    expect(search).to_have_attribute("aria-invalid", "true")
    page.locator(".vf-clear").click()
    expect(search).to_have_value("")
    expect(page.locator(".vf-search-error")).to_be_hidden()
    expect(page.locator(".vf-total strong")).to_have_text("5")


def test_searches_file_metadata_and_folders(live_server, page):
    db = live_server["db"]
    with db.conn:
        db.conn.execute("UPDATE photos SET exif_data=? WHERE filename='hawk1.jpg'", (
            json.dumps({"IPTC": {"Caption-Abstract": "Morning light", "City": "Monterey"},
                        "EXIF": {"Artist": "Jane Photographer"}}),
        ))
    search = open_browse(page, live_server)
    search.fill('"Morning light" AND Monterey')
    expect(page.locator(".vf-total strong")).to_have_text("1")
    expect(page.locator("#grid .grid-card")).to_have_count(1)
    search.fill("yard OR Photographer")
    expect(page.locator(".vf-total strong")).to_have_text("3")
    search.fill('"light Morning"')
    expect(page.locator(".vf-total strong")).to_have_text("0")


def test_keywords_toggle_limits_search_to_keyword_names(live_server, page):
    search = open_browse(page, live_server)
    search.fill("hawk")
    expect(page.locator(".vf-total strong")).to_have_text("3")
    toggle = page.locator(".vf-search-scope")
    with page.expect_response(lambda r: r.request.method == 'PUT' and '/api/workspaces/' in r.url):
        toggle.click()
    expect(toggle).to_have_attribute("aria-pressed", "true")
    # Only hawk1 carries a Hawk keyword; the filename/prediction matches drop.
    expect(page.locator(".vf-total strong")).to_have_text("1")
    expect(page.locator(".vf-chip-row")).to_contain_text("Keywords: “hawk”")
    page.reload()
    expect(page.locator(".vf-search input")).to_have_value("hawk")
    expect(page.locator(".vf-search-scope")).to_have_attribute("aria-pressed", "true")
    expect(page.locator(".vf-total strong")).to_have_text("1")
    page.locator(".vf-search-scope").click()
    expect(page.locator(".vf-total strong")).to_have_text("3")
    expect(page.locator(".vf-chip-row")).to_contain_text("Search: “hawk”")


def test_collection_editor_supports_metadata_rules(live_server, page):
    open_browse(page, live_server)
    page.get_by_role("button", name="+ New Collection").click()
    modal = page.locator("#collectionModal")
    modal.locator('#ruleRows select:has(option[value="metadata"])').select_option("metadata")
    value = modal.locator("#ruleRows input[type='text']")
    value.fill("Red-tailed Hawk")
    value.press("Enter")
    expect(modal.locator("#rulePreview")).to_have_text("Matches: 3 photos")
    modal.locator('#ruleRows select:has(option[value="not_contains"])').select_option("not_contains")
    expect(modal.locator("#rulePreview")).to_have_text("Matches: 2 photos")


def test_new_search_cancels_the_superseded_one_quietly(live_server, page, monkeypatch):
    """Typing a new search stops the server's work on the old one.

    The first term's SQL is made to take many seconds. When the user types a
    second term, the grid query and the summary for the first must come back
    as quiet ``search_superseded`` 409s well before they could have finished,
    with no error toast, while the grid and summary show the second search.
    """
    import threading

    import metadata_search

    original = metadata_search.photo_metadata_predicates
    slow_started = threading.Semaphore(0)

    def slow_for_one_term(like, term):
        parts, params = original(like, term)
        if term == "slowterm":
            slow_started.release()
            # Uncorrelated, so SQLite evaluates it once per statement:
            # tens of seconds unless the progress handler interrupts it.
            parts.append(
                "(SELECT COUNT(*) FROM (WITH RECURSIVE n(i) AS (SELECT 1 UNION ALL "
                "SELECT i + 1 FROM n WHERE i < 300000000) SELECT i FROM n)) < 0"
            )
        return parts, params

    monkeypatch.setattr(metadata_search, "photo_metadata_predicates", slow_for_one_term)
    search = open_browse(page, live_server)

    search.fill("slowterm")
    for _ in range(2):  # the grid query and the summary
        assert slow_started.acquire(timeout=10), "slow search never reached the server"

    def superseded(path):
        return lambda r: path in r.url and r.status == 409

    with page.expect_response(superseded("/api/photos/query"), timeout=10000) as grid, \
            page.expect_response(superseded("/api/browse/summary"), timeout=10000) as summary:
        search.fill("robin")
    assert grid.value.json()["code"] == "search_superseded"
    assert summary.value.json()["code"] == "search_superseded"

    expect(page.locator(".vf-total strong")).to_have_text("2")
    expect(page.locator("#summaryPhotoCount")).to_have_text("2")
    expect(page.locator('#toastContainer [data-type="error"]')).to_have_count(0)
