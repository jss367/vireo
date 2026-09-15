"""E2E: universal filter bar on Browse (Phase 2).

Ports the interaction checks from the design prototype's verify suite
(docs/plans/2026-07-19-photo-filter-prototype/verify_features.py) against
the real page: chip semantics, quick-filter multi-select, single
replaceable quick-search clause, pause/resume, typeahead counts,
persistence, and select-all consistency with the filtered grid.

Seed data (conftest): 3 hawk photos in /photos/park, 2 robins in
/photos/yard; hawk1 has rating 4 and the Red-tailed Hawk species keyword.
"""

import json

import pytest
from playwright.sync_api import expect


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


def test_browse_view_preferences_persist_across_navigation(live_server, page):
    _open_browse(page, live_server)

    page.locator("#sortSelect").select_option("name_desc")
    page.locator("#thumbSizeSlider").fill("300")

    page.goto(live_server["url"] + "/")
    _open_browse(page, live_server)

    assert page.locator("#sortSelect").input_value() == "name_desc"
    assert page.locator("#thumbSizeSlider").input_value() == "300"
    assert page.locator("#grid").evaluate(
        "el => el.style.getPropertyValue('--thumb-size')"
    ) == "300px"


def test_collection_open_waits_for_filter_bar_initialization(live_server, page):
    """An early collection click is queued while filter fields are loading."""
    collection_id = next(
        collection["id"]
        for collection in live_server["db"].get_collections()
        if collection["name"] == "GPS Without Location Keyword"
    )
    held_routes = []
    page.route(
        "**/api/filters/fields",
        lambda route: held_routes.append(route),
    )

    page.goto(live_server["url"] + "/browse")
    page.wait_for_selector("#grid .grid-card", timeout=15000)
    for _ in range(50):
        if held_routes:
            break
        page.wait_for_timeout(100)
    assert held_routes, "filter-field request was never issued"
    assert not page.evaluate("VireoFilter.isReady()")

    # Do not return the promise from page.evaluate: the collection open must
    # remain pending until the held registry request is released.
    page.evaluate(
        "collectionId => { window._earlyCollectionOpen = "
        "filterByCollection(collectionId); }",
        collection_id,
    )
    held_routes[0].continue_()

    page.wait_for_function(
        "collectionId => VireoFilter.isReady() && "
        "openedCollectionId === collectionId && VireoFilter.hasFilters()",
        arg=collection_id,
        timeout=15000,
    )


def test_queued_collection_open_yields_to_later_folder_click(live_server, page):
    """A queued early collection open must not clobber a later folder click."""
    collection_id = next(
        collection["id"]
        for collection in live_server["db"].get_collections()
        if collection["name"] == "GPS Without Location Keyword"
    )
    folder_id = live_server["data"]["folders"][1]
    held_routes = []
    page.route(
        "**/api/filters/fields",
        lambda route: held_routes.append(route),
    )

    page.goto(live_server["url"] + "/browse")
    page.wait_for_selector("#grid .grid-card", timeout=15000)
    for _ in range(50):
        if held_routes:
            break
        page.wait_for_timeout(100)
    assert held_routes, "filter-field request was never issued"
    assert not page.evaluate("VireoFilter.isReady()")

    # Early collection click queues behind the pending filter-bar init...
    page.evaluate(
        "collectionId => { window._earlyCollectionOpen = "
        "filterByCollection(collectionId); }",
        collection_id,
    )
    # ...but a later folder click changes scope before it can resume. The
    # queued collection open must observe the newer scope and abort instead
    # of overwriting the folder view.
    page.evaluate("folderId => filterByFolder(folderId)", folder_id)
    held_routes[0].continue_()

    page.wait_for_function("VireoFilter.isReady()", timeout=15000)
    # Give the queued collection continuation a chance to run so we can
    # assert it aborted rather than clobbering activeFolderId/openedCollectionId.
    page.wait_for_timeout(200)
    assert page.evaluate("activeFolderId") == folder_id
    assert page.evaluate("openedCollectionId") is None
    assert page.evaluate("activeCollectionId") is None


def test_queued_collection_open_yields_to_later_keyword_click(live_server, page):
    """A queued early collection open must not resume over a later keyword click.

    filterByKeyword is now queued behind browseFilterInitPromise (Codex review
    r3624927534) — the gen bump at entry invalidates the queued
    filterByCollection so it aborts, and the keyword rule is applied after
    fields load rather than dropped, so the user's later click is honored.
    """
    collection_id = next(
        collection["id"]
        for collection in live_server["db"].get_collections()
        if collection["name"] == "GPS Without Location Keyword"
    )
    held_routes = []
    page.route(
        "**/api/filters/fields",
        lambda route: held_routes.append(route),
    )

    page.goto(live_server["url"] + "/browse")
    page.wait_for_selector("#grid .grid-card", timeout=15000)
    for _ in range(50):
        if held_routes:
            break
        page.wait_for_timeout(100)
    assert held_routes, "filter-field request was never issued"
    assert not page.evaluate("VireoFilter.isReady()")

    # Early collection click queues behind the pending filter-bar init...
    page.evaluate(
        "collectionId => { window._earlyCollectionOpen = "
        "filterByCollection(collectionId); }",
        collection_id,
    )
    # ...but a later keyword click races in before the fields resolve. It
    # advances browseScopeGen so the queued collection open observes a newer
    # scope and aborts, then queues itself and applies once init resolves.
    page.evaluate(
        "() => { window._earlyKeywordClick = filterByKeyword('Red-tailed Hawk'); }"
    )
    held_routes[0].continue_()

    # The keyword rule must actually land after init resolves — that's the
    # difference from the old drop-the-click behavior, which left a
    # deep-linked collection (or bootstrap grid) showing while the user's
    # keyword selection was silently lost.
    page.wait_for_function(
        "() => VireoFilter.isReady() && activeKeyword === 'Red-tailed Hawk' && "
        "VireoFilter.hasFilters()",
        timeout=15000,
    )
    # Give the queued collection continuation a chance to run so we can
    # assert it aborted rather than clobbering the keyword selection.
    page.wait_for_timeout(200)
    assert page.evaluate("openedCollectionId") is None
    assert page.evaluate("activeCollectionId") is None
    assert page.evaluate("activeKeyword") == "Red-tailed Hawk"


def test_deep_link_replay_yields_to_sidebar_click(live_server, page):
    """?collection_id=A must not overwrite a sidebar click on collection B.

    Browsing to /browse?collection_id=A registers a post-init
    filterByCollection(activeCollectionId) as the deep-link replay. A sidebar
    click on collection B while /api/filters/fields is still pending queues
    behind the same init promise. Because the .then handler was registered
    first, it resumes first and, before the fix, its unconditional
    filterByCollection(A) call bumped browseScopeGen — invalidating B's
    queued open, so A opened over the user's real click (Codex review
    r3624637674).
    """
    collections_by_name = {c["name"]: c["id"] for c in live_server["db"].get_collections()}
    # Both collections must return non-empty grids: All Photos matches the
    # 5 seeded photos, Untagged matches the 3 without a keyword. Empty
    # collections would leave #grid .grid-card missing and hang the page
    # load before we even queue the sidebar click.
    deep_link_id = collections_by_name["All Photos"]
    clicked_id = collections_by_name["Untagged"]
    assert deep_link_id != clicked_id

    held_routes = []
    page.route(
        "**/api/filters/fields",
        lambda route: held_routes.append(route),
    )

    page.goto(live_server["url"] + f"/browse?collection_id={deep_link_id}")
    page.wait_for_selector("#grid .grid-card", timeout=15000)
    for _ in range(50):
        if held_routes:
            break
        page.wait_for_timeout(100)
    assert held_routes, "filter-field request was never issued"
    assert not page.evaluate("VireoFilter.isReady()")

    # User clicks a different collection in the sidebar while init is still
    # pending — this queues behind browseFilterInitPromise.
    page.evaluate(
        "collectionId => { window._userCollectionClick = "
        "filterByCollection(collectionId); }",
        clicked_id,
    )
    held_routes[0].continue_()

    page.wait_for_function(
        "clickedId => VireoFilter.isReady() && openedCollectionId === clickedId",
        arg=clicked_id,
        timeout=15000,
    )
    # The deep-link replay must have skipped itself — otherwise it would
    # have reopened deep_link_id after B's queued open bailed as stale.
    page.wait_for_timeout(200)
    assert page.evaluate("openedCollectionId") == clicked_id


def test_collection_count_and_optional_grid_keep_offline_members(live_server, page):
    """Collections retain offline members while Browse keeps them read-only."""
    db = live_server["db"]
    offline_folder = live_server["data"]["folders"][1]
    offline_ids = set(live_server["data"]["photos"][3:])
    db.conn.execute(
        "UPDATE folders SET status = 'missing' WHERE id = ?",
        (offline_folder,),
    )
    db.conn.commit()
    collection_id = next(
        c["id"] for c in db.get_collections() if c["name"] == "All Photos"
    )

    page.goto(live_server["url"] + "/browse")
    row = page.locator(
        f'#collectionList .tree-item[data-collection-id="{collection_id}"]'
    )
    expect(row.locator(".count")).to_contain_text("5")
    expect(row.locator(".collection-offline-count")).to_have_text("· 2 offline")

    row.click()
    notice = page.locator("#offlineCollectionNotice")
    expect(notice).to_be_visible(timeout=5000)
    expect(page.locator("#offlineCollectionText")).to_have_text(
        "3 of 5 photos available · 2 offline (hidden)"
    )
    expect(page.locator("#grid .grid-card")).to_have_count(3)

    page.locator("#offlineCollectionToggle").click()
    expect(page.locator("#grid .grid-card")).to_have_count(5)
    expect(page.locator("#grid .grid-card.offline")).to_have_count(2)
    assert set(page.locator("#grid .grid-card.offline").evaluate_all(
        "cards => cards.map(card => Number(card.dataset.id))"
    )) == offline_ids
    expect(page.locator("#grid .grid-card.offline img")).to_have_count(0)

    # Select all remains an operational action over accessible photos only.
    page.evaluate("selectAllMatchingPhotos()")
    page.wait_for_function("selectedPhotos.size === 3")
    assert set(page.evaluate("Array.from(selectedPhotos)")) == (
        set(live_server["data"]["photos"]) - offline_ids
    )


def test_all_offline_collection_does_not_show_import_welcome(live_server, page):
    """An opened collection remains an active scope when no files are online."""
    db = live_server["db"]
    db.conn.execute("UPDATE folders SET status = 'missing'")
    db.conn.commit()
    collection_id = next(
        collection["id"]
        for collection in db.get_collections()
        if collection["name"] == "All Photos"
    )

    page.goto(f"{live_server['url']}/browse?collection_id={collection_id}")

    expect(page.locator("#offlineCollectionNotice")).to_be_visible(timeout=15000)
    expect(page.locator("#offlineCollectionText")).to_have_text(
        "0 of 5 photos available · 5 offline (hidden)"
    )
    expect(page.locator("#grid .grid-card")).to_have_count(0)
    expect(page.locator("#welcomeState")).to_be_hidden()
    expect(page.locator("#emptyState")).to_be_visible()

    page.locator("#offlineCollectionToggle").click()
    expect(page.locator("#grid .grid-card.offline")).to_have_count(5)
    expect(page.locator("#emptyState")).to_be_hidden()


def test_pending_keyword_click_supersedes_deep_link_collection(live_server, page):
    """A keyword clicked during pending init must displace the URL collection.

    Opening ``/browse?collection_id=A`` starts the collection-scoped grid
    while ``/api/filters/fields`` is still pending. A keyword click during
    that window used to be dropped at the readiness guard — the bootstrap
    ``.then`` then saw ``scopeChanged`` and skipped the deep-link replay,
    but the collection A grid stayed on screen and the keyword rule was
    never installed (Codex review r3624927534). filterByKeyword now queues
    behind the init promise and applies once fields load, replacing the
    collection scope with the user's later intent.
    """
    collections_by_name = {c["name"]: c["id"] for c in live_server["db"].get_collections()}
    deep_link_id = collections_by_name["All Photos"]
    held_routes = []
    page.route(
        "**/api/filters/fields",
        lambda route: held_routes.append(route),
    )

    page.goto(live_server["url"] + f"/browse?collection_id={deep_link_id}")
    page.wait_for_selector("#grid .grid-card", timeout=15000)
    for _ in range(50):
        if held_routes:
            break
        page.wait_for_timeout(100)
    assert held_routes, "filter-field request was never issued"
    assert not page.evaluate("VireoFilter.isReady()")

    # Keyword click during pending init: previously dropped at the readiness
    # guard, now queued behind browseFilterInitPromise.
    page.evaluate(
        "() => { window._earlyKeywordClick = filterByKeyword('Red-tailed Hawk'); }"
    )
    held_routes[0].continue_()

    # After init, the keyword rule must land and the collection scope must
    # be gone — the queued keyword continuation clears activeCollectionId
    # and installs the rule. Only hawk1 carries the Red-tailed Hawk keyword
    # tag (predictions don't tag), so the total drops from All Photos (5)
    # to 1. The gap between "All Photos" and "keyword=Red-tailed Hawk"
    # totals is what makes the fix observable.
    page.wait_for_function(
        "() => VireoFilter.isReady() && activeKeyword === 'Red-tailed Hawk' && "
        "activeCollectionId === null && VireoFilter.hasFilters()",
        timeout=15000,
    )
    _wait_total(page, 1)


def test_restored_url_filters_apply_after_pending_folder_click(live_server, page):
    """URL/persisted filter chips must run through the grid after a pending
    sidebar click, not just render in the bar. Before the fix, the bootstrap
    ``.then`` returned early on ``scopeChanged`` and skipped the post-init
    ``VireoFilter.hasFilters()`` reload, so a folder click during pending
    ``/api/filters/fields`` produced a folder-scoped grid with the URL rating
    filter visible in the chip strip but never actually applied until the
    user edited a chip (Codex review r3624766665).
    """
    yard_folder_id = live_server["data"]["folders"][1]
    held_routes = []
    page.route(
        "**/api/filters/fields",
        lambda route: held_routes.append(route),
    )

    # rating_min=4 restores as a "rating >= 4" chip; only hawk1 (park) has
    # rating 4, so yard ∩ rating>=4 = 0 while yard alone = 2. That gap is
    # what makes the fix observable.
    page.goto(live_server["url"] + "/browse?rating_min=4")
    # Wait for the sidebar to render so the folder click hits a real
    # tree item; #gridContainer is present unconditionally and doesn't
    # prove bootstrap has populated folders yet.
    page.wait_for_function(
        "folderId => document.querySelector("
        "'#folderTree .tree-item[data-folder-id=\"' + folderId + '\"]')",
        arg=yard_folder_id,
        timeout=15000,
    )
    for _ in range(50):
        if held_routes:
            break
        page.wait_for_timeout(100)
    assert held_routes, "filter-field request was never issued"
    assert not page.evaluate("VireoFilter.isReady()")

    # Click yard folder while filter-bar init is pending. filterByFolder
    # bumps browseScopeGen and runs reloadBrowseResults() with no rules yet,
    # so the grid shows the 2 yard photos unfiltered by rating.
    page.evaluate("folderId => filterByFolder(folderId)", yard_folder_id)
    held_routes[0].continue_()

    # After init, the bootstrap .then must have applied the restored rating
    # rule against the yard folder scope; no yard photo has rating 4.
    page.wait_for_function(
        "folderId => VireoFilter.isReady() && VireoFilter.hasFilters() && "
        "activeFolderId === folderId",
        arg=yard_folder_id,
        timeout=15000,
    )
    _wait_total(page, 0)


@pytest.mark.parametrize("registry_fails", [False, True])
def test_shortcuts_wait_for_initialization(live_server, page, registry_fails):
    """Visible shortcuts stay disabled while metadata loads or fails."""
    held_routes = []
    page.route("**/api/filters/fields", lambda route: held_routes.append(route))
    page.goto(live_server["url"] + "/browse")
    page.wait_for_selector("#grid .grid-card", timeout=15000)
    buttons = page.locator(".vf-shortcuts button")
    for button in buttons.all():
        expect(button).to_be_visible()
        expect(button).to_be_disabled()
    # A native disabled button does not dispatch a click before handlers exist.
    page.locator('[data-missing="has_species"]').evaluate("button => button.click()")
    with page.expect_response("**/api/filters/fields"):
        if registry_fails:
            held_routes[0].fulfill(status=503, json={"error": "Metadata unavailable"})
        else:
            held_routes[0].continue_()
    if registry_fails:
        for button in buttons.all():
            expect(button).to_be_disabled()
        assert not page.evaluate("VireoFilter.isReady()")
    else:
        for button in buttons.all():
            expect(button).to_be_enabled()
        _wait_total(page, 5)
        page.locator('[data-missing="has_species"]').click()
        _wait_total(page, 3)


def test_quick_rating_filter_and_chip_semantics(live_server, page):
    _open_browse(page, live_server)
    assert _total(page) == 5

    # Rating and color stay in the popover; tag and flag shortcuts are visible.
    expect(page.locator(".vf-quick")).to_be_hidden()
    page.click(".vf-filters-btn")
    expect(page.locator(".vf-quick")).to_be_visible()
    assert page.locator('.vf-quick-rating .vf-star[data-rating="4"]').is_visible()
    page.click('.vf-quick-rating .vf-star[data-rating="4"]')
    _wait_total(page, 1)
    chips = page.evaluate("document.querySelector('.vf-chips').textContent")
    assert "Rating is at least 4 stars" in chips
    # Toggling the same star clears the rule.
    page.click('.vf-quick-rating .vf-star[data-rating="4"]')
    _wait_total(page, 5)


def test_quick_flags_multi_select_combines(live_server, page):
    _open_browse(page, live_server)
    expect(page.locator(".vf-popover")).to_be_hidden()
    assert page.locator('.vf-quick-flags [data-flag="flagged"]').is_visible()
    page.click('.vf-quick-flags [data-flag="flagged"]')
    _wait_total(page, 0)
    expect(page.locator('[data-flag="flagged"]')).to_have_attribute("aria-pressed", "true")
    page.click('.vf-quick-flags [data-flag="none"]')
    # Seed photos have NULL flags — all 5 must count as Unflagged.
    _wait_total(page, 5)
    chips = page.evaluate("document.querySelector('.vf-chips').textContent")
    assert "Flag is one of Picked, Unflagged" in chips


def test_missing_tag_shortcuts_combine_and_distinguish_gps(live_server, page):
    db = live_server["db"]
    photos = live_server["data"]["photos"]
    place = db.add_keyword("City Park", kw_type="location")
    # A named place without coordinates counts as tagged; GPS alone does not.
    db.tag_photo(photos[0], place)
    db.tag_photo(photos[1], place)
    with db.conn:
        db.conn.execute(
            "UPDATE photos SET latitude=37.7, longitude=-122.4 WHERE id=?",
            (photos[2],),
        )
    _open_browse(page, live_server)
    species = page.locator('[data-missing="has_species"]')
    location = page.locator('[data-missing="has_location_keyword"]')
    expect(species).to_be_visible()
    expect(location).to_be_visible()
    expect(page.locator(".vf-popover")).to_be_hidden()

    # Pending AI predictions on all five photos do not count as species tags.
    species.click()
    _wait_total(page, 3)
    location.click()
    _wait_total(page, 4)
    expect(page.locator(".vf-missing-hint")).to_be_visible()
    expect(page.locator(".vf-chips")).to_contain_text(
        "Missing species OR Missing location tag"
    )

    # Existing search and flag filters still narrow the missing-either set.
    page.locator(".vf-search input").fill("hawk")
    _wait_total(page, 2)
    page.locator('[data-flag="flagged"]').click()
    _wait_total(page, 0)
    page.locator('[data-flag="flagged"]').click()
    _wait_total(page, 2)
    page.locator(".vf-search input").fill("")
    _wait_total(page, 4)

    species.click()
    _wait_total(page, 3)
    expect(species).to_have_attribute("aria-pressed", "false")
    expect(location).to_have_attribute("aria-pressed", "true")
    expect(page.locator(".vf-missing-hint")).to_be_hidden()
    location.click()
    _wait_total(page, 5)


@pytest.mark.parametrize("shortcut", ["missing", "flag"])
def test_shortcuts_narrow_collection_with_any_rules(live_server, page, shortcut):
    db = live_server["db"]
    photos = live_server["data"]["photos"]
    collection_id = db.add_collection("Two hawk photos", json.dumps({
        "mode": "any",
        "rules": [
            {"field": "filename", "op": "is", "value": "hawk1.jpg"},
            {"field": "filename", "op": "is", "value": "hawk2.jpg"},
        ],
    }))
    with db.conn:
        db.conn.execute("UPDATE photos SET flag='flagged' WHERE id IN (?, ?)",
                        (photos[1], photos[4]))
    _open_browse(page, live_server)
    page.evaluate("id => filterByCollection(id)", collection_id)
    _wait_total(page, 2)
    original = page.evaluate("VireoFilter.getUserRules()")
    url = page.url
    button = page.locator('[data-missing="has_species"]' if shortcut == "missing"
                          else '[data-flag="flagged"]')
    button.click()
    _wait_total(page, 1)
    expect(page.locator("#grid .grid-card")).to_have_count(1)
    assert page.url == url
    # Toggling off restores the collection's original set and OR expression.
    button.click()
    _wait_total(page, 2)
    assert original in page.evaluate("VireoFilter.getUserRules().rules")


@pytest.mark.parametrize("grouped", [False, True])
def test_missing_shortcuts_preserve_multiple_existing_clauses(live_server, page, grouped):
    """Adding a shortcut must not reduce missing-both rules to missing-either."""
    db = live_server["db"]
    photos = live_server["data"]["photos"]
    place = db.add_keyword("City Park", kw_type="location")
    db.tag_photo(photos[0], place)
    db.tag_photo(photos[1], place)
    _open_browse(page, live_server)
    missing_species = {"field": "has_species", "op": "is", "value": 0}
    original = {"mode": "all", "rules": [
        {"mode": "any", "rules": [missing_species]} if grouped else missing_species,
        {"field": "has_location_keyword", "op": "is", "value": 0},
    ]}
    page.evaluate("rules => VireoFilter.loadExpression(rules)", original)
    _wait_total(page, 2)
    species = page.locator('[data-missing="has_species"]')
    location = page.locator('[data-missing="has_location_keyword"]')
    expect(species).to_have_attribute("aria-pressed", "false")
    expect(location).to_have_attribute("aria-pressed", "false")
    location.click()
    expect(location).to_have_attribute("aria-pressed", "true")
    species.click()
    expect(species).to_have_attribute("aria-pressed", "true")
    assert original in page.evaluate("VireoFilter.getUserRules().rules")
    _wait_total(page, 2)
    # The added shortcut must also remain removable without changing the base.
    location.click()
    species.click()
    expect(location).to_have_attribute("aria-pressed", "false")
    expect(species).to_have_attribute("aria-pressed", "false")
    assert page.evaluate("VireoFilter.getUserRules().rules") == [original]
    _wait_total(page, 2)


@pytest.mark.parametrize("mode", ["any", "none"])
@pytest.mark.parametrize("field,value,selector", [
    ("flag", "flagged", '[data-flag="flagged"]'),
    ("color_label", "red", '.vf-quick-colors [data-color="red"]'),
])
def test_enum_shortcuts_preserve_advanced_root(live_server, page, mode, field, value, selector):
    """An OR/NOT leaf must not look like a selected narrowing shortcut."""
    db = live_server["db"]
    photo_id = live_server["data"]["photos"][0]
    if field == "flag":
        db.update_photo_flag(photo_id, value)
    else:
        db.set_color_label(photo_id, value)
    _open_browse(page, live_server)
    original = {"mode": mode, "rules": [
        {"field": field, "op": "is", "value": value},
        {"field": "filename", "op": "is", "value": "hawk2.jpg"},
    ]}
    page.evaluate("rules => VireoFilter.loadExpression(rules)", original)
    _wait_total(page, 2 if mode == "any" else 3)
    if field == "color_label":
        page.locator(".vf-filters-btn").click()
    button = page.locator(selector)
    expect(button).not_to_have_class("active")
    button.click()
    _wait_total(page, 1 if mode == "any" else 0)
    expect(button).to_have_class("active")
    button.click()
    _wait_total(page, 2 if mode == "any" else 3)
    expect(button).not_to_have_class("active")
    assert page.evaluate("VireoFilter.getUserRules().rules") == [original]


def test_missing_tag_shortcuts_restore_pause_and_clear(live_server, page):
    _open_browse(page, live_server)
    species = page.locator('[data-missing="has_species"]')
    location = page.locator('[data-missing="has_location_keyword"]')
    species.click()
    _wait_total(page, 3)
    with page.expect_response(lambda response: "/api/workspaces/" in response.url
                              and response.request.method == "PUT"):
        location.click()
    _wait_total(page, 5)
    page.reload()
    page.wait_for_function("VireoFilter.isReady()")
    expect(species).to_have_attribute("aria-pressed", "true")
    expect(location).to_have_attribute("aria-pressed", "true")
    page.locator(".vf-mute").click()
    expect(page.locator(".vf-shortcuts")).to_have_class("vf-shortcuts muted")
    page.locator(".vf-mute").click()
    page.locator(".vf-chip-x").click()
    expect(species).to_have_attribute("aria-pressed", "false")
    expect(location).to_have_attribute("aria-pressed", "false")
    _wait_total(page, 5)


def test_quick_search_is_single_replaceable_clause(live_server, page):
    _open_browse(page, live_server)
    search = page.locator(".vf-search input")
    # Partial text filters live; no Enter or suggestion click is required.
    search.fill("haw")
    _wait_total(page, 3)
    search.fill("rob")
    _wait_total(page, 2)
    chips = page.evaluate("document.querySelector('.vf-chips').textContent")
    assert "rob" in chips and "haw" not in chips


def test_typing_keeps_active_visual_clause(live_server, page):
    """Editing the search box while a visual clause is active must not
    silently convert it to a text search — the box shows the visual prompt,
    so live-applying mid-edit would swap the clause out from under the user.
    Switching modes stays an explicit action (Enter or the suggestion
    dropdown)."""
    _open_browse(page, live_server)
    search = page.locator(".vf-search input")
    search.fill("hawk")
    page.wait_for_selector(".vf-search-suggest button", timeout=8000)
    page.locator('.vf-search-suggest [data-search-kind="visual"]').click()
    page.wait_for_selector(".vf-chip.visual", timeout=8000)

    # Edit the prompt; the visual chip must survive the live-search debounce.
    search.fill("hawk flying")
    page.wait_for_timeout(400)
    assert page.locator(".vf-chip.visual").count() == 1
    chips = page.evaluate("document.querySelector('.vf-chips').textContent")
    assert "Search:" not in chips

    # Enter is the explicit switch to a text search.
    search.press("Enter")
    page.wait_for_function(
        "!document.querySelector('.vf-chip.visual')", timeout=8000,
    )
    chips = page.evaluate("document.querySelector('.vf-chips').textContent")
    assert "hawk flying" in chips


def test_clear_cancels_pending_live_search_debounce(live_server, page):
    """Clear during the debounce must not silently reinstate the typed text.

    Regression for Codex review r3791783342: the 150 ms `quickSearchTimer`
    from an in-progress live-search keystroke used to fire after `.vf-clear`
    reset the state, silently reinstalling the search after the UI reported
    "Filters cleared."
    """
    _open_browse(page, live_server)
    # Establish an active filter so `.vf-clear` is visible in the top bar.
    page.click(".vf-filters-btn")
    page.click('.vf-quick-rating .vf-star[data-rating="4"]')
    _wait_total(page, 1)
    page.click(".vf-done")

    search = page.locator(".vf-search input")
    search.fill("haw")
    # Click Clear before the 150 ms debounce fires; the pending timer must
    # be cancelled, not left to overwrite the just-cleared state.
    page.click(".vf-clear")
    page.wait_for_timeout(400)
    assert not page.evaluate("VireoFilter.hasFilters()")
    assert search.input_value() == ""
    _wait_total(page, 5)


@pytest.mark.parametrize("clear_action", ["top", "popover", "api"])
def test_clearing_filters_keeps_selected_photo_in_place(
    live_server, page, clear_action
):
    """Clear-all widens the grid around the current photo without resetting it."""
    _open_browse(page, live_server)
    selected_id = live_server["data"]["photos"][3]
    page.evaluate("updateThumbSize(400)")

    # Use a normal rule rather than quick search so this exercises the
    # explicit clear-all reason instead of quickSearchCleared.
    page.evaluate("VireoFilter.addRule('keyword', 'is', 'American Robin')")
    page.wait_for_function("() => photos.length === 1")
    selected = page.locator(f'.grid-card[data-id="{selected_id}"]')
    selected.click()

    top_before = page.evaluate(
        """(id) => {
          const card = document.querySelector(`.grid-card[data-id="${id}"]`);
          const container = document.getElementById('gridContainer');
          return card.getBoundingClientRect().top - container.getBoundingClientRect().top;
        }""",
        selected_id,
    )

    if clear_action == "popover":
        page.click(".vf-filters-btn")
        page.click(".vf-clear-rules")
    elif clear_action == "api":
        page.evaluate("VireoFilter.clearAll()")
    else:
        page.click(".vf-clear")
    page.wait_for_function(
        "(id) => photos.length === 5 && selectedPhotoId === id",
        arg=selected_id,
    )
    page.wait_for_timeout(100)  # allow the anchor-restoration animation frame

    assert page.evaluate("selectedPhotos.size") == 0
    expect(selected).to_have_class("grid-card selected")
    top_after = page.evaluate(
        """(id) => {
          const card = document.querySelector(`.grid-card[data-id="${id}"]`);
          const container = document.getElementById('gridContainer');
          return card.getBoundingClientRect().top - container.getBoundingClientRect().top;
        }""",
        selected_id,
    )
    assert abs(top_after - top_before) < 4


@pytest.mark.parametrize("clear_action", ["popover", "api"])
def test_clear_without_filters_does_not_reload(live_server, page, clear_action):
    """No-op clears cancel pending text without reloading the photo dataset."""
    _open_browse(page, live_server)
    selected = page.locator(".grid-card").last
    selected.click()
    selected_id = int(selected.get_attribute("data-id"))
    epoch_before = page.evaluate("loadEpoch")

    if clear_action == "popover":
        page.click(".vf-filters-btn")
    page.evaluate(
        """action => {
          var input = document.querySelector('.vf-search input');
          input.value = 'haw';
          input.dispatchEvent(new Event('input', {bubbles: true}));
          if (action === 'popover') {
            document.querySelector('.vf-clear-rules').click();
          } else {
            VireoFilter.clearAll();
          }
        }""",
        clear_action,
    )
    page.wait_for_timeout(300)

    assert page.evaluate("loadEpoch") == epoch_before
    assert not page.evaluate("VireoFilter.hasFilters()")
    assert page.locator(".vf-search input").input_value() == ""
    assert page.evaluate("selectedPhotoId") == selected_id
    expect(selected).to_have_class("grid-card selected")


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


def test_save_as_collection_and_reopen(live_server, page):
    """Phase 5: the bar's expression saves as a Collection and reopens into
    the bar as editable chips (rules + visual round-trip)."""
    _open_browse(page, live_server)
    search = page.locator(".vf-search input")
    search.fill("hawk")
    search.press("Enter")
    _wait_total(page, 3)
    page.evaluate("VireoFilter.visualSearch('a soaring hawk')")
    page.wait_for_selector(".vf-chip.visual", timeout=8000)

    page.click(".vf-filters-btn")
    page.click(".vf-save-collection")
    page.wait_for_selector(".vf-save-modal:not([hidden])", timeout=8000)
    preview = page.inner_text(".vf-save-preview")
    assert "Visually similar" in preview
    page.fill(".vf-save-name", "Soaring hawks")
    page.click(".vf-save-confirm")
    page.wait_for_function(
        "window.collectionsById && Object.values(collectionsById)"
        ".some(c => c.name === 'Soaring hawks')",
        timeout=8000,
    )
    # Sidebar row carries the visual marker.
    page.wait_for_selector(".collection-visual-mark", timeout=8000)

    # Clear everything, then reopen the collection into the bar.
    page.click(".vf-done")
    page.click(".vf-clear")
    page.wait_for_function(
        "!VireoFilter.hasFilters()", timeout=8000,
    )
    cid = page.evaluate(
        "Object.values(collectionsById).find(c => c.name === 'Soaring hawks').id"
    )
    page.evaluate(f"filterByCollection({cid})")
    page.wait_for_selector(".vf-chip.visual", timeout=8000)
    chips = page.evaluate("document.querySelector('.vf-chips').textContent")
    assert "a soaring hawk" in chips
    # Quick-search group round-trips too (the hawk text clause).
    assert "hawk" in chips
    assert page.evaluate("VireoFilter.getVisual().prompt") == "a soaring hawk"


@pytest.mark.parametrize("width", [1440, 1000])
def test_compact_header_and_floating_selection_actions(live_server, page, width):
    page.set_viewport_size({"width": width, "height": 900})
    _open_browse(page, live_server)
    header = page.locator(".browse-filter-shell")
    grid = page.locator("#gridContainer")
    grid_top = grid.bounding_box()["y"]
    # Allow the new shortcut row (which wraps at the narrower window size).
    assert header.bounding_box()["height"] < (145 if width == 1440 else 240)
    primary = page.locator(".vf-primary").bounding_box()
    shortcuts = page.locator(".vf-shortcuts").bounding_box()
    secondary = page.locator(".vf-secondary").bounding_box()
    assert primary["y"] + primary["height"] <= shortcuts["y"]
    assert shortcuts["y"] + shortcuts["height"] <= secondary["y"]
    expect(page.locator(".vf-quick")).to_be_hidden()
    expect(page.locator(".vf-overflow")).to_be_hidden()

    page.click(".vf-filters-btn")
    expect(page.locator(".vf-quick")).to_be_visible()
    assert grid.bounding_box()["y"] == grid_top
    page.click(".vf-done")
    expect(page.locator(".vf-quick")).to_be_hidden()

    page.locator("#grid .grid-card").first.click()
    bar = page.locator("#batchBar")
    expect(bar).to_be_visible()
    assert grid.bounding_box()["y"] == grid_top
    assert bar.bounding_box()["y"] > grid.bounding_box()["y"]
    pane = page.locator(".content-area").bounding_box()
    box = bar.bounding_box()
    assert pane["x"] <= box["x"]
    assert box["x"] + box["width"] <= pane["x"] + pane["width"]
    grid.evaluate("el => { el.scrollTop = el.scrollHeight; }")
    page.wait_for_function("""() => {
        const cards = document.querySelectorAll('#grid .grid-card');
        return cards[cards.length - 1].getBoundingClientRect().bottom <=
            document.getElementById('batchBar').getBoundingClientRect().top;
    }""")
    bar.get_by_role("button", name="More", exact=False).click()
    expect(page.locator(".vireo-ctx-menu")).to_be_visible()
    page.keyboard.press("Escape")
    bar.get_by_role("button", name="Clear", exact=True).click()
    expect(bar).to_be_hidden()

    page.click(".vf-filters-btn")
    page.click('.vf-star[data-rating="4"]')
    _wait_total(page, 1)
    page.click(".vf-done")
    expect(page.locator(".vf-count")).to_have_text("1")
    expect(page.locator(".vf-chips")).to_contain_text("Rating is at least 4 stars")
    assert page.locator("#vireoFilterBar").evaluate("""bar => {
        const right = bar.getBoundingClientRect().right;
        return [...bar.querySelectorAll('.vf-primary > *, .vf-shortcuts button, .vf-chip-row > *')]
            .filter(el => el.getBoundingClientRect().width)
            .every(el => el.getBoundingClientRect().right <= right + 1);
    }""")
