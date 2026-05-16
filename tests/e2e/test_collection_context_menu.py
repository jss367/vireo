"""E2E tests for the collection sidebar right-click context menu (Task 9).

Menu items:
- Filter by this Collection
- separator
- Rename
- Duplicate
- separator
- Delete Collection
"""

from playwright.sync_api import expect


def _seed_collection(live_server, name="Test Pick"):
    """Create a collection via the Flask test client directly (avoids racy
    page.evaluate(fetch) seeding)."""
    import json as _json
    # Use requests-style client through the live_server's db: easiest path is
    # adding the row directly via the bound db handle, since that matches what
    # other e2e helpers do.
    db = live_server["db"]
    return db.add_collection(name, _json.dumps([]))


def test_collection_right_click_shows_menu(live_server, page):
    """Right-clicking a collection tree-item opens the collection menu."""
    _seed_collection(live_server, "Test Pick")
    url = live_server["url"]
    page.goto(f"{url}/browse")

    item = page.locator(
        ".tree-item[data-collection-id]", has_text="Test Pick"
    ).first
    item.wait_for(state="visible")
    item.click(button="right")

    menu = page.locator(".vireo-ctx-menu")
    expect(menu).to_be_visible()
    for label in [
        "Filter by this Collection",
        "Rename",
        "Duplicate",
        "Delete Collection",
    ]:
        expect(
            menu.locator(".vireo-ctx-item", has_text=label)
        ).to_be_visible()


def test_collection_filter_fires_filter(live_server, page):
    """Clicking 'Filter by this Collection' sets activeCollectionId."""
    cid = _seed_collection(live_server, "Picks A")
    url = live_server["url"]
    page.goto(f"{url}/browse")

    item = page.locator(
        ".tree-item[data-collection-id]", has_text="Picks A"
    ).first
    item.wait_for(state="visible")
    item.click(button="right")

    menu = page.locator(".vireo-ctx-menu")
    expect(menu).to_be_visible()
    menu.locator(
        ".vireo-ctx-item", has_text="Filter by this Collection"
    ).click()
    expect(menu).to_be_hidden()

    page.wait_for_function(
        f"window.activeCollectionId === {cid}", timeout=3000
    )


def test_collection_click_preserves_selected_member_position(live_server, page):
    """Switching into a collection should keep a selected member anchored.

    Regression: filterByCollection() cleared selectedPhotoId before loading the
    collection, so a focused photo disappeared from selection even when it was
    present in the destination collection.
    """
    import json as _json

    db = live_server["db"]
    target_id = live_server["data"]["photos"][3]
    cid = db.add_collection(
        "All Test Photos",
        _json.dumps([{"field": "all"}]),
    )

    url = live_server["url"]
    page.goto(f"{url}/browse")

    card = page.locator(f'.grid-card[data-id="{target_id}"]')
    card.wait_for(state="visible")
    card.click()

    before_top = page.evaluate(
        """(photoId) => {
          const card = document.querySelector(`.grid-card[data-id="${photoId}"]`);
          const container = document.getElementById('gridContainer');
          return Math.round(card.getBoundingClientRect().top - container.getBoundingClientRect().top);
        }""",
        target_id,
    )

    page.locator(
        ".tree-item[data-collection-id]", has_text="All Test Photos"
    ).click()

    page.wait_for_function(
        f"window.activeCollectionId === {cid} && window.selectedPhotoId === {target_id}",
        timeout=3000,
    )
    page.locator(f'.grid-card[data-id="{target_id}"]').wait_for(state="visible")
    assert page.evaluate(
        """(photoId) => document
          .querySelector(`.grid-card[data-id="${photoId}"]`)
          .classList.contains('selected')""",
        target_id,
    )

    page.wait_for_function(
        """([photoId, beforeTop]) => {
          const card = document.querySelector(`.grid-card[data-id="${photoId}"]`);
          const container = document.getElementById('gridContainer');
          if (!card || !container) return false;
          const top = Math.round(card.getBoundingClientRect().top - container.getBoundingClientRect().top);
          return Math.abs(top - beforeTop) <= 1;
        }""",
        arg=[target_id, before_top],
        timeout=3000,
    )
    after_top = page.evaluate(
        """(photoId) => {
          const card = document.querySelector(`.grid-card[data-id="${photoId}"]`);
          const container = document.getElementById('gridContainer');
          return Math.round(card.getBoundingClientRect().top - container.getBoundingClientRect().top);
        }""",
        target_id,
    )
    assert abs(after_top - before_top) <= 1


def test_stale_collection_switch_does_not_restore_anchor(live_server, page):
    """An older collection switch must not restore after a newer switch wins."""
    import json as _json

    db = live_server["db"]
    first_id = db.add_collection(
        "First Collection",
        _json.dumps([{"field": "all"}]),
    )
    second_id = db.add_collection(
        "Second Collection",
        _json.dumps([{"field": "all"}]),
    )
    target_id = live_server["data"]["photos"][3]

    url = live_server["url"]
    page.goto(f"{url}/browse")

    page.locator(f'.grid-card[data-id="{target_id}"]').wait_for(state="visible")
    page.locator(f'.grid-card[data-id="{target_id}"]').click()

    page.evaluate(
        """() => {
          perPage = 1;
          window.__restoreAnchorCalls = 0;
          const realRestore = window.restorePhotoAnchor;
          window.restorePhotoAnchor = function(anchor) {
            window.__restoreAnchorCalls += 1;
            return realRestore(anchor);
          };

          const realLoadPhotos = window.loadPhotos;
          let calls = 0;
          window.__releaseFirstCollectionLoad = null;
          window.loadPhotos = async function() {
            calls += 1;
            if (calls === 1) {
              await new Promise(resolve => {
                window.__releaseFirstCollectionLoad = resolve;
              });
            }
            return realLoadPhotos();
          };
        }"""
    )

    page.evaluate("(collectionId) => { window.__firstSwitch = filterByCollection(collectionId); }", first_id)
    page.wait_for_function("typeof window.__releaseFirstCollectionLoad === 'function'")

    page.evaluate("window.selectedPhotoId = null")
    page.evaluate("(collectionId) => filterByCollection(collectionId)", second_id)
    page.wait_for_function(f"window.activeCollectionId === {second_id}")

    page.evaluate("window.__releaseFirstCollectionLoad()")
    page.wait_for_function("window.__firstSwitch && true")
    page.evaluate("() => window.__firstSwitch")
    page.wait_for_timeout(50)

    assert page.evaluate("window.activeCollectionId") == second_id
    assert page.evaluate("window.__restoreAnchorCalls") == 0


def test_collection_anchor_does_not_override_new_selection(live_server, page):
    """Delayed anchor restore must not clobber a user's newer selection."""
    import json as _json

    db = live_server["db"]
    collection_id = db.add_collection(
        "Delayed Collection",
        _json.dumps([{"field": "all"}]),
    )
    original_id = live_server["data"]["photos"][3]
    new_id = live_server["data"]["photos"][0]

    url = live_server["url"]
    page.goto(f"{url}/browse")

    page.locator(f'.grid-card[data-id="{original_id}"]').wait_for(state="visible")
    page.locator(f'.grid-card[data-id="{original_id}"]').click()

    page.evaluate(
        """() => {
          perPage = 1;
          window.__restoreAnchorCalls = 0;
          const realRestore = window.restorePhotoAnchor;
          window.restorePhotoAnchor = function(anchor) {
            window.__restoreAnchorCalls += 1;
            return realRestore(anchor);
          };

          const realLoadPhotos = window.loadPhotos;
          let calls = 0;
          window.__releaseSecondLoad = null;
          window.loadPhotos = async function() {
            calls += 1;
            if (calls === 2) {
              await new Promise(resolve => {
                window.__releaseSecondLoad = resolve;
              });
            }
            return realLoadPhotos();
          };
        }"""
    )

    page.evaluate(
        "(collectionId) => { window.__switchPromise = filterByCollection(collectionId); }",
        collection_id,
    )
    page.wait_for_function("typeof window.__releaseSecondLoad === 'function'")

    page.evaluate(
        """(photoId) => {
          selectedPhotoId = photoId;
          selectedIndex = photos.findIndex(p => p.id === photoId);
        }""",
        new_id,
    )

    page.evaluate("window.__releaseSecondLoad()")
    page.evaluate("() => window.__switchPromise")
    page.wait_for_timeout(50)

    assert page.evaluate("window.selectedPhotoId") == new_id
    assert page.evaluate("window.__restoreAnchorCalls") == 0


def test_collection_duplicate_fires_endpoint_and_rerenders(live_server, page):
    """Clicking 'Duplicate' POSTs to /duplicate and re-renders the list."""
    _seed_collection(live_server, "Picks B")
    url = live_server["url"]
    page.goto(f"{url}/browse")

    item = page.locator(
        ".tree-item[data-collection-id]", has_text="Picks B"
    ).first
    item.wait_for(state="visible")
    item.click(button="right")

    menu = page.locator(".vireo-ctx-menu")
    expect(menu).to_be_visible()

    with page.expect_response(
        lambda r: "/duplicate" in r.url and r.status == 200
    ):
        menu.locator(".vireo-ctx-item", has_text="Duplicate").click()

    # After duplicate, the list re-renders with the copy visible.
    expect(
        page.locator(
            ".tree-item[data-collection-id]", has_text="Picks B (copy)"
        )
    ).to_be_visible()


def test_collection_rename_fires_put(live_server, page):
    """Clicking 'Rename' prompts and PUTs the new name."""
    _seed_collection(live_server, "Picks C")
    url = live_server["url"]
    page.goto(f"{url}/browse")

    item = page.locator(
        ".tree-item[data-collection-id]", has_text="Picks C"
    ).first
    item.wait_for(state="visible")

    # Accept the rename prompt with a new name. Register BEFORE clicking the
    # menu item (the dialog may fire synchronously).
    page.on("dialog", lambda d: d.accept("Picks C Renamed"))

    item.click(button="right")
    menu = page.locator(".vireo-ctx-menu")
    expect(menu).to_be_visible()

    with page.expect_response(
        lambda r: "/api/collections/" in r.url
        and r.request.method == "PUT"
        and r.status == 200
    ):
        menu.locator(".vireo-ctx-item", has_text="Rename").click()

    # List re-renders with the new name.
    expect(
        page.locator(
            ".tree-item[data-collection-id]", has_text="Picks C Renamed"
        )
    ).to_be_visible()


def test_collection_delete_fires_endpoint(live_server, page):
    """Clicking 'Delete Collection' confirms and DELETEs."""
    cid = _seed_collection(live_server, "Picks D")
    url = live_server["url"]
    page.goto(f"{url}/browse")

    item = page.locator(
        ".tree-item[data-collection-id]", has_text="Picks D"
    ).first
    item.wait_for(state="visible")

    # Auto-accept the confirm dialog BEFORE clicking.
    page.on("dialog", lambda d: d.accept())

    item.click(button="right")
    menu = page.locator(".vireo-ctx-menu")
    expect(menu).to_be_visible()

    with page.expect_response(
        lambda r: r.url.endswith(f"/api/collections/{cid}")
        and r.request.method == "DELETE"
        and r.status == 200
    ):
        menu.locator(".vireo-ctx-item", has_text="Delete Collection").click()

    # List re-renders without the deleted collection.
    expect(
        page.locator(
            ".tree-item[data-collection-id]", has_text="Picks D"
        )
    ).to_have_count(0)
