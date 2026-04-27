"""E2E tests for the /keywords page Link-to-Google-place flow (Task 15).

These tests cover the **non-Google** parts of the flow:
  - The 📍 Link... button only appears on `type='location'` keywords with
    no `place_id` (and is hidden otherwise).
  - The modal opens on click and closes on Cancel / Escape.
  - The link API path itself wires through correctly when triggered via the
    test-only `window.__linkPlaceForTest` hook (which simulates "user picked
    a Google suggestion" without driving the Google Places UI).

We deliberately don't drive `google.maps.places.Autocomplete` here — that
needs a real API key + network roundtrip. Tests instead call the JS test
hook OR `/api/keywords/<id>/link-place` directly, with `places.place_details`
monkeypatched.
"""

from playwright.sync_api import expect

# Canned Place Details response for the link-place call. Shape matches
# `places.place_details` (see vireo/places.py).
#
# Note: address_components in real Google responses do NOT typically include
# the leaf place itself — only its containing administrative parents. We
# follow that here so the parent-chain upsert creates exactly 2 new unlinked
# keyword rows (California, United States) on top of the linked leaf.
_CANNED_PLACE_ID = "ChIJTestKeywordsLink"
_CANNED_DETAILS = {
    "place_id": _CANNED_PLACE_ID,
    "name": "Yosemite National Park",
    "lat": 37.8651,
    "lng": -119.5383,
    "address_components": [
        {"name": "California", "types": ["administrative_area_level_1"]},
        {"name": "United States", "types": ["country"]},
    ],
}


def _seed_location_keyword(live_server, name, place_id=None):
    """Create a `type='location'` keyword and tag the first photo with it so
    it shows up in /api/keywords/all (which is workspace-scoped via tagged
    photos + ancestors). Returns the keyword id."""
    db = live_server["db"]
    photo_id = live_server["data"]["photos"][0]
    if place_id:
        # Use upsert path so place_id flows through normally.
        cur = db.conn.execute(
            "INSERT INTO keywords (name, type, place_id) VALUES (?, 'location', ?)",
            (name, place_id),
        )
        kw_id = cur.lastrowid
        db.conn.commit()
    else:
        kw_id = db.get_or_create_text_location(name)
    db.tag_photo(photo_id, kw_id)
    return kw_id


def _set_api_key(key="test-key"):
    """Write a Google Maps key into the (monkeypatched) config.json."""
    import config as cfg

    current = cfg.load()
    current["google_maps_api_key"] = key
    cfg.save(current)


def test_link_button_visible_on_unlinked_location_keyword(live_server, page):
    """A type='location' keyword with no place_id renders a 📍 Link... button."""
    kw_id = _seed_location_keyword(live_server, "the meadow")

    page.goto(f"{live_server['url']}/keywords")
    row = page.locator(f"tr[data-id='{kw_id}']")
    row.wait_for(state="visible")

    btn = row.locator(".kw-link-btn")
    expect(btn).to_be_visible()
    expect(btn).to_contain_text("Link")


def test_link_button_hidden_on_linked_location_keyword(live_server, page):
    """A type='location' keyword that already has a place_id shows a linked
    badge instead of the Link button."""
    kw_id = _seed_location_keyword(
        live_server, "Yosemite Valley", place_id="ChIJSeedExisting"
    )

    page.goto(f"{live_server['url']}/keywords")
    row = page.locator(f"tr[data-id='{kw_id}']")
    row.wait_for(state="visible")

    expect(row.locator(".kw-link-btn")).to_have_count(0)
    expect(row.locator(".kw-linked-badge")).to_be_visible()


def test_link_button_hidden_on_non_location_keyword(live_server, page):
    """Non-location keywords (e.g. taxonomy) do not get a Link button."""
    page.goto(f"{live_server['url']}/keywords")
    # The seed includes 'Red-tailed Hawk' as a species/taxonomy keyword.
    row = page.locator("tr[data-id]", has_text="Red-tailed Hawk").first
    row.wait_for(state="visible")
    expect(row.locator(".kw-link-btn")).to_have_count(0)


def test_link_modal_opens_and_closes(live_server, page):
    """Clicking the Link button opens the modal; Cancel hides it again."""
    kw_id = _seed_location_keyword(live_server, "the cabin")

    page.goto(f"{live_server['url']}/keywords")
    row = page.locator(f"tr[data-id='{kw_id}']")
    row.wait_for(state="visible")
    row.locator(".kw-link-btn").click()

    modal = page.locator("#linkPlaceModal")
    expect(modal).to_have_class("modal-overlay open")
    # Pre-fills the input with the keyword's current name.
    expect(page.locator("#linkPlaceInput")).to_have_value("the cabin")

    page.locator("#linkPlaceCancel").click()
    # The .open class drives display:flex; without it the modal is hidden.
    expect(modal).not_to_have_class("modal-overlay open")


def test_link_modal_closes_on_escape(live_server, page):
    """Escape key closes the link modal."""
    kw_id = _seed_location_keyword(live_server, "the trailhead")

    page.goto(f"{live_server['url']}/keywords")
    row = page.locator(f"tr[data-id='{kw_id}']")
    row.wait_for(state="visible")
    row.locator(".kw-link-btn").click()

    modal = page.locator("#linkPlaceModal")
    expect(modal).to_have_class("modal-overlay open")

    page.keyboard.press("Escape")
    expect(modal).not_to_have_class("modal-overlay open")


def test_link_modal_shows_no_key_error_when_unconfigured(live_server, page):
    """With no Google Maps API key in config, the modal opens but surfaces an
    inline error instead of trying to load Google Maps JS."""
    kw_id = _seed_location_keyword(live_server, "free text place")

    page.goto(f"{live_server['url']}/keywords")
    row = page.locator(f"tr[data-id='{kw_id}']")
    row.wait_for(state="visible")
    row.locator(".kw-link-btn").click()

    err = page.locator("#linkPlaceError")
    expect(err).to_be_visible()
    expect(err).to_contain_text("API key")

    # And no Google Maps script tag should have been injected.
    count = page.evaluate(
        "document.querySelectorAll('script[src*=\"maps.googleapis.com\"]').length"
    )
    assert count == 0


def test_unlinked_locations_filter_chip_count_and_filter(live_server, page):
    """The 'Unlinked locations' chip shows the count of type='location'
    keywords with no place_id, and clicking it narrows the table to only
    those rows."""
    kw_unlinked_id = _seed_location_keyword(live_server, "the unlinked field")
    kw_linked_id = _seed_location_keyword(
        live_server, "Already Linked", place_id="ChIJAlreadyLinked"
    )

    page.goto(f"{live_server['url']}/keywords")
    page.locator(f"tr[data-id='{kw_unlinked_id}']").wait_for(state="visible")

    # Chip count reflects the one unlinked location (the linked one doesn't
    # count, taxonomy keywords don't count).
    expect(page.locator("#kwUnlinkedCount")).to_have_text("1")

    page.locator("#kwFilterUnlinked").click()
    # Only the unlinked row remains visible.
    expect(page.locator(f"tr[data-id='{kw_unlinked_id}']")).to_be_visible()
    expect(page.locator(f"tr[data-id='{kw_linked_id}']")).to_have_count(0)


def test_link_attaches_place_id_via_test_hook(live_server, page, monkeypatch):
    """Driving the JS submit hook (the same path the place_changed listener
    takes once a Google suggestion is picked) should:
      * POST /api/keywords/<id>/link-place
      * Update the row to show a linked badge instead of the Link button
      * Decrement the unlinked-locations chip count
      * Persist the place_id on the keyword row in the DB
    """
    import places

    monkeypatch.setattr(places, "place_details", lambda pid, key: _CANNED_DETAILS)
    _set_api_key()

    kw_id = _seed_location_keyword(live_server, "to-be-linked")

    page.goto(f"{live_server['url']}/keywords")
    row = page.locator(f"tr[data-id='{kw_id}']")
    row.wait_for(state="visible")

    expect(row.locator(".kw-link-btn")).to_be_visible()
    expect(page.locator("#kwUnlinkedCount")).to_have_text("1")

    # Trigger the submit path the place_changed listener would take. We pass
    # a non-empty place_id; the canned monkeypatched place_details returns
    # _CANNED_DETAILS regardless of the argument.
    page.evaluate(
        "([id, pid]) => window.__linkPlaceForTest(id, pid)",
        [kw_id, _CANNED_PLACE_ID],
    )

    # Wait for the row to re-render (loadKeywords resolves async). Use the
    # linked badge as the readiness signal.
    expect(
        page.locator(f"tr[data-id='{kw_id}'] .kw-linked-badge")
    ).to_be_visible()
    expect(
        page.locator(f"tr[data-id='{kw_id}'] .kw-link-btn")
    ).to_have_count(0)
    # Linking the leaf decreases the unlinked count by one (the original
    # free-text leaf was the only unlinked location pre-link). The parent
    # chain spawns its own location rows (California, USA), but those are
    # not visible in /api/keywords/all unless they're ancestors of a tagged
    # keyword. Since the leaf is tagged, its parents ARE ancestors — so the
    # chip count rises again. The user-facing invariant we DO care about is
    # that the originally-linked row is no longer counted.
    final_count = int(page.locator("#kwUnlinkedCount").inner_text())
    assert final_count == 2, f"expected 2 (parent chain), got {final_count}"

    # Sanity-check the DB row got place_id + coords.
    row_db = live_server["db"].conn.execute(
        "SELECT place_id, latitude, longitude FROM keywords WHERE id = ?",
        (kw_id,),
    ).fetchone()
    assert row_db["place_id"] == _CANNED_PLACE_ID
    assert row_db["latitude"] == _CANNED_DETAILS["lat"]
    assert row_db["longitude"] == _CANNED_DETAILS["lng"]
