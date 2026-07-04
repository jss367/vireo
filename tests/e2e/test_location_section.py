"""E2E tests for the photo detail Location section in browse.html.

These tests exercise the section in the no-Google-API-key branch (the live_server
fixture starts with an empty config), so they cover the free-text Enter path
and the server-side initial render. They do NOT test Google Places autocomplete
itself — that requires a real key and a network round-trip we don't want in CI.

The EXIF-suggestion tests below seed `place_reverse_geocode_cache` directly so
the server's `/api/places/reverse-geocode` proxy serves a hit without ever
calling Google. The Accept-button test additionally monkeypatches
`places.place_details` so the POST /api/photos/<id>/location call returns canned
data without a live API call.
"""

import json

from playwright.sync_api import expect

# Canned reverse-geocode response: shape matches what `places.reverse_geocode`
# produces (see vireo/places.py). `_summarize_details` reads `name` plus the
# last 1-2 entries of `address_components`.
_CANNED_PLACE_ID = "ChIJTestCentralPark"
_CANNED_DETAILS = {
    "place_id": _CANNED_PLACE_ID,
    "name": "Central Park, New York, NY, USA",
    "lat": 40.785091,
    "lng": -73.968285,
    "address_components": [
        {"name": "Central Park", "types": ["park"]},
        {"name": "New York", "types": ["locality"]},
        {"name": "United States", "types": ["country"]},
    ],
}


def _seed_exif_photo(live_server, lat=40.785091, lng=-73.968285):
    """Set lat/lng on the first seeded photo and return its id."""
    db = live_server["db"]
    photo_id = live_server["data"]["photos"][0]
    with db.conn:
        db.conn.execute(
            "UPDATE photos SET latitude = ?, longitude = ? WHERE id = ?",
            (lat, lng, photo_id),
        )
    return photo_id


def _seed_exif_photos(live_server, photo_ids, lat=40.785091, lng=-73.968285):
    """Set lat/lng on the given photos."""
    db = live_server["db"]
    with db.conn:
        for photo_id in photo_ids:
            db.conn.execute(
                "UPDATE photos SET latitude = ?, longitude = ? WHERE id = ?",
                (lat, lng, photo_id),
            )


def _seed_reverse_geocode_cache(live_server, lat, lng, place_id, details):
    """Pre-populate `place_reverse_geocode_cache` so the proxy serves a hit."""
    live_server["db"].reverse_geocode_cache_put(
        lat, lng, place_id=place_id, response_json=json.dumps(details),
    )


def _wait_for_detail_loaded(page):
    """Wait until the detail panel's photo id is set on `window`.

    `_submitLocationText` (browse.html) bails silently if `_detailPhotoId` is
    falsy, so pressing Enter before the detail finishes loading races and the
    free-text POST is dropped. Tests must wait for this before pressing Enter.
    """
    page.wait_for_function("() => !!window._detailPhotoId")


def _set_api_key(key="test-key"):
    """Write a Google Maps key into the (monkeypatched) config.json so:
      - browse.html's _cfgPromise sees `window.GOOGLE_MAPS_API_KEY` set,
        which gates the `maybeShowExifSuggestion` fetch path; AND
      - the server's reverse-geocode + set-location routes accept the request.
    """
    import config as cfg

    current = cfg.load()
    current["google_maps_api_key"] = key
    cfg.save(current)


def test_location_section_renders_empty(live_server, page):
    """Opening a photo with no location shows the empty input."""
    url = live_server["url"]
    page.goto(f"{url}/browse")

    first = page.locator(".grid-card").first
    first.wait_for(state="visible")
    first.click()

    section = page.locator("#locationSection")
    expect(section).to_be_visible()
    expect(page.locator("#locationInput")).to_be_visible()
    expect(page.locator("#locationFilled")).to_be_hidden()


def test_freetext_enter_creates_location(live_server, page):
    """Typing a free-text location and pressing Enter swaps to the filled state."""
    url = live_server["url"]
    page.goto(f"{url}/browse")

    first = page.locator(".grid-card").first
    first.wait_for(state="visible")
    first.click()
    _wait_for_detail_loaded(page)

    inp = page.locator("#locationInput")
    inp.wait_for(state="visible")
    inp.fill("the meadow behind the cabin")
    inp.press("Enter")

    filled = page.locator("#locationFilled")
    expect(filled).to_be_visible()
    expect(page.locator("#locationFilled .filled-place")).to_have_text(
        "the meadow behind the cabin"
    )
    # Free-text locations have no parent chain.
    expect(page.locator("#locationFilled .filled-parents")).to_have_count(0)
    expect(page.locator("#locationEmpty")).to_be_hidden()


def test_existing_location_keywords_suggest_while_typing(live_server, page):
    """The Location input shows saved location keywords separately from Google."""
    db = live_server["db"]
    photo_ids = live_server["data"]["photos"][:2]
    existing_location_id = db.get_or_create_text_location("San Diego Airbnb")
    db.set_photo_location(photo_ids[1], existing_location_id)

    page.goto(f"{live_server['url']}/browse")
    page.locator(f".grid-card[data-id='{photo_ids[0]}']").wait_for(state="visible")
    page.locator(f".grid-card[data-id='{photo_ids[0]}']").click()
    _wait_for_detail_loaded(page)

    inp = page.locator("#locationInput")
    inp.wait_for(state="visible")
    inp.fill("SA")

    suggestions = page.locator("#locationKeywordSuggestions")
    expect(suggestions).to_be_visible()
    expect(suggestions).to_contain_text("San Diego Airbnb")
    expect(suggestions).to_contain_text("Saved location")

    suggestions.locator(".keyword-suggestion-option", has_text="San Diego Airbnb").click()

    expect(page.locator("#locationFilled")).to_be_visible()
    expect(page.locator("#locationFilled .filled-place")).to_have_text(
        "San Diego Airbnb"
    )
    row = db.conn.execute(
        "SELECT 1 FROM photo_keywords "
        "WHERE photo_id = ? AND keyword_id = ?",
        (photo_ids[0], existing_location_id),
    ).fetchone()
    assert row is not None


def test_saved_location_enter_does_not_steal_google_keyboard_pick(live_server, page):
    """With a Google key configured, Enter remains available to Google Places."""
    db = live_server["db"]
    photo_ids = live_server["data"]["photos"][:2]
    existing_location_id = db.get_or_create_text_location("San Diego Airbnb")
    db.set_photo_location(photo_ids[1], existing_location_id)
    _set_api_key()

    page.goto(f"{live_server['url']}/browse")
    page.locator(f".grid-card[data-id='{photo_ids[0]}']").wait_for(state="visible")
    page.locator(f".grid-card[data-id='{photo_ids[0]}']").click()
    _wait_for_detail_loaded(page)

    inp = page.locator("#locationInput")
    inp.wait_for(state="visible")
    inp.fill("San")
    expect(page.locator("#locationKeywordSuggestions")).to_be_visible()

    # Simulate Google place_changed having just handled the keyboard pick.
    page.evaluate("window._locationLastPickedAt = Date.now();")
    inp.press("Enter")
    page.wait_for_timeout(600)

    row = db.conn.execute(
        "SELECT 1 FROM photo_keywords "
        "WHERE photo_id = ? AND keyword_id = ?",
        (photo_ids[0], existing_location_id),
    ).fetchone()
    assert row is None
    expect(page.locator("#locationEmpty")).to_be_visible()


def test_enter_after_place_changed_does_not_submit_freetext(live_server, page):
    """Regression for the keyboard-selected-autocomplete race: when the
    user presses Enter to pick a highlighted Google suggestion, the
    keydown event fires SYNCHRONOUSLY before place_changed. Without the
    deferred-cancel logic, the keydown handler would POST a free-text
    submit before place_changed could route the place_id POST.

    We simulate place_changed firing (by bumping _locationLastPickedAt
    just before pressing Enter) and verify NO free-text POST is sent.
    """
    url = live_server["url"]
    page.goto(f"{url}/browse")

    first = page.locator(".grid-card").first
    first.wait_for(state="visible")
    first.click()
    _wait_for_detail_loaded(page)

    inp = page.locator("#locationInput")
    inp.wait_for(state="visible")
    inp.fill("Central Park")

    # Track network calls so we can assert the text endpoint never fires.
    text_posts = []
    page.on("request", lambda req: text_posts.append(req.url) if (
        req.method == "POST" and "/location/text" in req.url
    ) else None)

    # Simulate place_changed having just fired — like a keyboard pick.
    page.evaluate("window._locationLastPickedAt = Date.now();")
    inp.press("Enter")

    # Wait past the 300ms defer window plus margin.
    page.wait_for_timeout(600)

    # No free-text POST should have happened. (We didn't simulate a real
    # place_id submit either, so the input remains in the empty state —
    # that's expected for this regression test.)
    assert text_posts == [], (
        f"keydown handler must defer to place_changed when "
        f"_locationLastPickedAt is recent; got POSTs: {text_posts}"
    )
    # Sanity: we should still be in the empty state (no submit happened).
    expect(page.locator("#locationEmpty")).to_be_visible()


def test_clear_button_returns_to_empty(live_server, page):
    """The × button on the filled state clears the location server-side."""
    url = live_server["url"]
    page.goto(f"{url}/browse")

    first = page.locator(".grid-card").first
    first.wait_for(state="visible")
    first.click()
    _wait_for_detail_loaded(page)

    # Set a location first.
    inp = page.locator("#locationInput")
    inp.wait_for(state="visible")
    inp.fill("test location")
    inp.press("Enter")

    expect(page.locator("#locationFilled")).to_be_visible()

    # Clear it.
    page.locator("#locationFilled .clear-location").click()

    expect(page.locator("#locationFilled")).to_be_hidden()
    expect(page.locator("#locationEmpty")).to_be_visible()


def test_location_persists_across_reload(live_server, page):
    """Server-side initial render: re-opening the photo after a reload shows the
    saved location without any client-side roundtrip beyond /api/photos/<id>."""
    url = live_server["url"]
    page.goto(f"{url}/browse")

    first = page.locator(".grid-card").first
    first.wait_for(state="visible")
    first.click()
    _wait_for_detail_loaded(page)

    inp = page.locator("#locationInput")
    inp.wait_for(state="visible")
    inp.fill("Persistent Place")
    inp.press("Enter")

    expect(page.locator("#locationFilled .filled-place")).to_have_text(
        "Persistent Place"
    )

    # Reload the whole page; the section should re-render filled when the
    # photo is reopened.
    page.reload()
    page.locator(".grid-card").first.wait_for(state="visible")
    page.locator(".grid-card").first.click()

    expect(page.locator("#locationFilled")).to_be_visible()
    expect(page.locator("#locationFilled .filled-place")).to_have_text(
        "Persistent Place"
    )


def test_no_gmaps_script_when_key_empty(live_server, page):
    """With no API key configured, focusing the input must NOT inject the
    Google Maps script tag — we shouldn't ping Google with an empty key."""
    url = live_server["url"]
    page.goto(f"{url}/browse")

    first = page.locator(".grid-card").first
    first.wait_for(state="visible")
    first.click()

    inp = page.locator("#locationInput")
    inp.wait_for(state="visible")
    inp.focus()

    # Give any binding a moment to fire (sync; no real network expected).
    page.wait_for_timeout(100)

    # No <script src> pointing at maps.googleapis.com should exist.
    count = page.evaluate(
        "document.querySelectorAll('script[src*=\"maps.googleapis.com\"]').length"
    )
    assert count == 0


def test_exif_suggestion_appears_when_photo_has_gps_and_no_location(live_server, page):
    """A photo with EXIF GPS + no location keyword + a configured API key
    should trigger the reverse-geocode proxy and render the suggestion line.

    We pre-populate `place_reverse_geocode_cache` so the proxy serves a hit
    without ever calling Google.
    """
    photo_id = _seed_exif_photo(live_server)
    _seed_reverse_geocode_cache(
        live_server, 40.785091, -73.968285, _CANNED_PLACE_ID, _CANNED_DETAILS,
    )
    _set_api_key()

    page.goto(f"{live_server['url']}/browse")
    card = page.locator(f".grid-card[data-id='{photo_id}']")
    card.wait_for(state="visible")
    card.click()

    sugg = page.locator("#locationExifSuggestion")
    expect(sugg).to_be_visible()
    expect(sugg).to_contain_text("EXIF says:")
    expect(sugg).to_contain_text("Central Park")
    expect(sugg.locator("button.accept-btn")).to_have_text("Accept")


def test_exif_suggestion_hidden_when_no_gps(live_server, page):
    """A photo with no lat/lng must NOT trigger the suggestion line, even if
    a key is configured."""
    _set_api_key()

    page.goto(f"{live_server['url']}/browse")
    page.locator(".grid-card").first.wait_for(state="visible")
    page.locator(".grid-card").first.click()

    page.locator("#locationInput").wait_for(state="visible")
    # Give the no-op JS path a moment in case anything would fire.
    page.wait_for_timeout(150)

    expect(page.locator("#locationExifSuggestion")).to_be_hidden()


def test_exif_suggestion_hidden_when_already_has_location(live_server, page):
    """Photos that already have a location keyword render the filled state,
    so the empty container (and the suggestion line within it) stays hidden."""
    db = live_server["db"]
    photo_id = _seed_exif_photo(live_server)
    # Pre-tag with a free-text location.
    leaf_id = db.get_or_create_text_location("Pre-existing Location")
    db.set_photo_location(photo_id, leaf_id)
    _seed_reverse_geocode_cache(
        live_server, 40.785091, -73.968285, _CANNED_PLACE_ID, _CANNED_DETAILS,
    )
    _set_api_key()

    page.goto(f"{live_server['url']}/browse")
    page.locator(".grid-card").first.wait_for(state="visible")
    page.locator(".grid-card").first.click()

    expect(page.locator("#locationFilled")).to_be_visible()
    expect(page.locator("#locationExifSuggestion")).to_be_hidden()


def test_exif_accept_button_attaches_location(live_server, page, monkeypatch):
    """Clicking Accept POSTs the place_id and switches to the filled state.

    POST /api/photos/<id>/location calls `places.place_details` (Google), so we
    monkeypatch that to return our canned details.
    """
    photo_id = _seed_exif_photo(live_server)
    _seed_reverse_geocode_cache(
        live_server, 40.785091, -73.968285, _CANNED_PLACE_ID, _CANNED_DETAILS,
    )
    _set_api_key()

    import places
    monkeypatch.setattr(
        places, "place_details", lambda pid, key: _CANNED_DETAILS,
    )

    page.goto(f"{live_server['url']}/browse")
    page.locator(".grid-card").first.wait_for(state="visible")
    page.locator(".grid-card").first.click()

    accept = page.locator("#locationExifSuggestion button.accept-btn")
    expect(accept).to_be_visible()
    accept.click()

    filled = page.locator("#locationFilled")
    expect(filled).to_be_visible()
    expect(page.locator("#locationFilled .filled-place")).to_have_text(
        _CANNED_DETAILS["name"]
    )
    expect(page.locator("#locationExifSuggestion")).to_be_hidden()

    # Sanity-check server state: the leaf keyword is now linked.
    row = live_server["db"].conn.execute(
        "SELECT 1 FROM photo_keywords pk "
        "JOIN keywords k ON k.id = pk.keyword_id "
        "WHERE pk.photo_id = ? AND k.type = 'location' AND k.place_id = ?",
        (photo_id, _CANNED_PLACE_ID),
    ).fetchone()
    assert row is not None


def test_exif_accept_batches_selection_and_refreshes_smart_collection(
    live_server, page, monkeypatch
):
    """Accepting an EXIF suggestion applies to the active selection and reloads
    the GPS-without-location smart collection.
    """
    photo_ids = live_server["data"]["photos"][:3]
    _seed_exif_photos(live_server, photo_ids)
    _seed_reverse_geocode_cache(
        live_server, 40.785091, -73.968285, _CANNED_PLACE_ID, _CANNED_DETAILS,
    )
    _set_api_key()

    import places
    monkeypatch.setattr(
        places, "place_details", lambda pid, key: _CANNED_DETAILS,
    )

    db = live_server["db"]
    collection_id = next(
        c["id"]
        for c in db.get_collections()
        if c["name"] == "GPS Without Location Keyword"
    )
    assert db.count_collection_photos(collection_id) == 3

    page.goto(f"{live_server['url']}/browse")
    page.locator(".grid-card").first.wait_for(state="visible")
    page.evaluate("(collectionId) => filterByCollection(collectionId)", collection_id)
    page.wait_for_function(
        "(collectionId) => activeCollectionId === collectionId && photos.length === 3",
        arg=collection_id,
    )

    page.locator(f".grid-card[data-id='{photo_ids[0]}']").click()
    _wait_for_detail_loaded(page)
    expect(page.locator("#locationExifSuggestion button.accept-btn")).to_be_visible()
    page.locator(f".grid-card[data-id='{photo_ids[1]}']").click(modifiers=["Meta"])
    page.locator(f".grid-card[data-id='{photo_ids[2]}']").click(modifiers=["Meta"])
    page.wait_for_function("() => selectedPhotos.size === 3")

    page.locator("#locationExifSuggestion button.accept-btn").click()
    page.wait_for_function("() => activeCollectionId !== null && photos.length === 0")

    for photo_id in photo_ids:
        row = db.conn.execute(
            "SELECT 1 FROM photo_keywords pk "
            "JOIN keywords k ON k.id = pk.keyword_id "
            "WHERE pk.photo_id = ? AND k.type = 'location' AND k.place_id = ?",
            (photo_id, _CANNED_PLACE_ID),
        ).fetchone()
        assert row is not None
        expect(page.locator(f".grid-card[data-id='{photo_id}']")).to_have_count(0)
    assert db.count_collection_photos(collection_id) == 0


def test_exif_suggestion_clears_when_anchor_leaves_selection(
    live_server, page, monkeypatch
):
    """Codex P1 (PR #1097): if the photo that produced the EXIF suggestion is
    removed from the selection before Accept is clicked, the suggestion must
    not survive to apply that anchor's GPS-derived location to unrelated
    photos still in the selection.

    Sequence: open A (suggestion appears) → Cmd-click B (suggestion stays,
    anchor still in selection) → Cmd-click A to remove → Cmd-click C. Now the
    selection is {B, C} and A is gone; the suggestion (which is A's) must be
    cleared so batch Accept can't silently push A's place to B and C.
    """
    photo_ids = live_server["data"]["photos"][:3]
    anchor, other_b, other_c = photo_ids
    # Give only the anchor EXIF GPS. If every photo shared the same canned
    # place_id, the bug would be invisible even without the fix.
    _seed_exif_photos(live_server, [anchor])
    _seed_reverse_geocode_cache(
        live_server, 40.785091, -73.968285, _CANNED_PLACE_ID, _CANNED_DETAILS,
    )
    _set_api_key()

    import places
    monkeypatch.setattr(
        places, "place_details", lambda pid, key: _CANNED_DETAILS,
    )

    db = live_server["db"]

    page.goto(f"{live_server['url']}/browse")
    page.locator(".grid-card").first.wait_for(state="visible")

    page.locator(f".grid-card[data-id='{anchor}']").click()
    _wait_for_detail_loaded(page)
    accept = page.locator("#locationExifSuggestion button.accept-btn")
    expect(accept).to_be_visible()
    assert page.evaluate(
        "() => document.getElementById('locationExifSuggestion').dataset.photoId"
    ) == str(anchor)

    page.locator(f".grid-card[data-id='{other_b}']").click(modifiers=["Meta"])
    page.wait_for_function(
        "(id) => selectedPhotos.size === 2 && selectedPhotos.has(id)",
        arg=anchor,
    )
    expect(accept).to_be_visible()

    page.locator(f".grid-card[data-id='{anchor}']").click(modifiers=["Meta"])
    page.wait_for_function(
        "(id) => selectedPhotos.size === 1 && !selectedPhotos.has(id)",
        arg=anchor,
    )

    page.locator(f".grid-card[data-id='{other_c}']").click(modifiers=["Meta"])
    page.wait_for_function(
        "(id) => selectedPhotos.size === 2 && !selectedPhotos.has(id)",
        arg=anchor,
    )

    expect(page.locator("#locationExifSuggestion")).to_be_hidden()
    assert page.evaluate(
        "() => document.getElementById('locationExifSuggestion').innerHTML"
    ) == ""
    assert page.evaluate(
        "() => document.getElementById('locationExifSuggestion').dataset.photoId || ''"
    ) == ""

    for pid in (other_b, other_c):
        row = db.conn.execute(
            "SELECT 1 FROM photo_keywords pk "
            "JOIN keywords k ON k.id = pk.keyword_id "
            "WHERE pk.photo_id = ? AND k.type = 'location'",
            (pid,),
        ).fetchone()
        assert row is None, f"photo {pid} should not have gained a location"


def test_exif_suggestion_cleared_by_filled_location_render(
    live_server, page
):
    """Codex P2 (PR #1097): opening a photo with a saved location renders the
    filled state without running maybeShowExifSuggestion, so a suggestion
    fetched for the previous photo used to survive (hidden) in #locationEmpty.
    Cmd-clicking that previous photo back into a batch would then resurrect
    its Accept line for a selection whose other member already has a location.

    Sequence: open A (suggestion appears) → open B with a saved location
    (filled render must clear A's suggestion) → Cmd-click A ({B, A} batch).
    The suggestion must stay gone.
    """
    photo_ids = live_server["data"]["photos"][:2]
    photo_a, photo_b = photo_ids
    _seed_exif_photos(live_server, [photo_a])
    _seed_reverse_geocode_cache(
        live_server, 40.785091, -73.968285, _CANNED_PLACE_ID, _CANNED_DETAILS,
    )
    _set_api_key()

    db = live_server["db"]
    existing_location_id = db.get_or_create_text_location("Existing Spot")
    db.set_photo_location(photo_b, existing_location_id)

    page.goto(f"{live_server['url']}/browse")
    page.locator(".grid-card").first.wait_for(state="visible")

    page.locator(f".grid-card[data-id='{photo_a}']").click()
    _wait_for_detail_loaded(page)
    expect(page.locator("#locationExifSuggestion button.accept-btn")).to_be_visible()

    page.locator(f".grid-card[data-id='{photo_b}']").click()
    expect(page.locator("#locationFilled")).to_be_visible()
    expect(page.locator("#locationFilled .filled-place")).to_have_text("Existing Spot")
    # The filled render must scrub the previous photo's suggestion entirely,
    # not just rely on #locationEmpty being hidden.
    assert page.evaluate(
        "() => document.getElementById('locationExifSuggestion').innerHTML"
    ) == ""
    assert page.evaluate(
        "() => document.getElementById('locationExifSuggestion').dataset.photoId || ''"
    ) == ""

    page.locator(f".grid-card[data-id='{photo_a}']").click(modifiers=["Meta"])
    page.wait_for_function("() => selectedPhotos.size === 2")

    expect(page.locator("#locationExifSuggestion")).to_be_hidden()
    assert page.evaluate(
        "() => document.getElementById('locationExifSuggestion').innerHTML"
    ) == ""

    # B's saved location is untouched.
    row = db.conn.execute(
        "SELECT 1 FROM photo_keywords WHERE photo_id = ? AND keyword_id = ?",
        (photo_b, existing_location_id),
    ).fetchone()
    assert row is not None, "photo B should keep its saved location"


def test_exif_suggestion_bails_when_slow_geocode_resolves_after_anchor_dropped(
    live_server, page, monkeypatch
):
    """Codex P1 (PR #1097, commit 9d06366c): the post-await path in
    maybeShowExifSuggestion only guarded on window._detailPhotoId, which
    batch mode does NOT update when the anchor is dropped from the
    selection. A slow reverse-geocode for photo A could still stamp and
    show A's Accept line for a selection that no longer includes A, and
    clicking it would apply A's place to unrelated photos.

    Sequence: open A (reverse-geocode is intercepted and held) →
    Cmd-click B → Cmd-click A out → Cmd-click C (selection is now
    {B, C}) → release A's reverse-geocode response. The suggestion must
    stay hidden and no other photo may gain a location.
    """
    photo_ids = live_server["data"]["photos"][:3]
    anchor, other_b, other_c = photo_ids
    # Only the anchor has GPS. B and C have no lat/lng, so their own
    # maybeShowExifSuggestion calls bail immediately and never race with
    # ours. If they shared A's canned place_id the bug would be masked.
    _seed_exif_photos(live_server, [anchor])
    _set_api_key()

    import places
    monkeypatch.setattr(
        places, "place_details", lambda pid, key: _CANNED_DETAILS,
    )

    db = live_server["db"]

    # Intercept the reverse-geocode call and hold it. Playwright dispatches
    # the route handler on a background task, so page interactions below
    # can proceed while the fetch stays pending in the browser.
    held_routes = []

    def _hold_reverse_geocode(route):
        held_routes.append(route)

    page.route("**/api/places/reverse-geocode**", _hold_reverse_geocode)

    page.goto(f"{live_server['url']}/browse")
    page.locator(".grid-card").first.wait_for(state="visible")

    page.locator(f".grid-card[data-id='{anchor}']").click()
    _wait_for_detail_loaded(page)

    # Wait for the browser to actually issue the reverse-geocode fetch.
    # maybeShowExifSuggestion awaits _cfgPromise before fetching, so this
    # is not synchronous with the click.
    for _ in range(50):
        if held_routes:
            break
        page.wait_for_timeout(100)
    assert held_routes, "reverse-geocode fetch was never issued"

    # While the response is held, drive the anchor-dropped batch sequence.
    page.locator(f".grid-card[data-id='{other_b}']").click(modifiers=["Meta"])
    page.wait_for_function(
        "(id) => selectedPhotos.size === 2 && selectedPhotos.has(id)",
        arg=anchor,
    )
    page.locator(f".grid-card[data-id='{anchor}']").click(modifiers=["Meta"])
    page.wait_for_function(
        "(id) => selectedPhotos.size === 1 && !selectedPhotos.has(id)",
        arg=anchor,
    )
    page.locator(f".grid-card[data-id='{other_c}']").click(modifiers=["Meta"])
    page.wait_for_function(
        "(id) => selectedPhotos.size === 2 && !selectedPhotos.has(id)",
        arg=anchor,
    )

    # Now release the response. Without the fix, the completion path would
    # stamp #locationExifSuggestion with A's place because _detailPhotoId
    # still equals A.
    held_routes[0].fulfill(
        status=200,
        content_type="application/json",
        body=json.dumps({
            "place_id": _CANNED_PLACE_ID,
            "summary": _CANNED_DETAILS["name"],
            "lat": _CANNED_DETAILS["lat"],
            "lng": _CANNED_DETAILS["lng"],
            "cached": False,
        }),
    )

    # Give the async completion a chance to run. If it were going to paint
    # the suggestion, it would happen synchronously after the fetch/JSON
    # awaits resolve.
    page.wait_for_timeout(300)

    expect(page.locator("#locationExifSuggestion")).to_be_hidden()
    assert page.evaluate(
        "() => document.getElementById('locationExifSuggestion').innerHTML"
    ) == ""
    assert page.evaluate(
        "() => document.getElementById('locationExifSuggestion').dataset.photoId || ''"
    ) == ""

    # And no photo in the surviving selection may have gained a location.
    for pid in (other_b, other_c):
        row = db.conn.execute(
            "SELECT 1 FROM photo_keywords pk "
            "JOIN keywords k ON k.id = pk.keyword_id "
            "WHERE pk.photo_id = ? AND k.type = 'location'",
            (pid,),
        ).fetchone()
        assert row is None, f"photo {pid} should not have gained a location"


def test_exif_suggestion_bails_when_slow_geocode_resolves_after_location_saved(
    live_server, page
):
    """Codex P2 (PR #1097, commit accf79ce): if the reverse-geocode fetch
    for photo A is still pending while the user saves a free-text location
    for A via the input, renderLocationFilled scrubs the suggestion element
    — but the pending completion path in maybeShowExifSuggestion only
    guarded on _detailPhotoId and the active selection, both of which are
    still A. So it re-stamped a stale Accept line into the (now-hidden)
    empty container. A later Cmd-click into a batch then preserved the
    suggestion (owner A is in selection) and revealed it, letting Accept
    overwrite A's saved location and push A's EXIF place to the other
    photo in the batch.

    Sequence: open A (reverse-geocode held) → type a free-text location
    for A and hit Enter → filled state renders → release the held
    reverse-geocode response → Cmd-click B. The suggestion must stay
    hidden and empty, and A's saved location must survive.
    """
    photo_ids = live_server["data"]["photos"][:2]
    anchor, other_b = photo_ids
    # Only the anchor has GPS. If B shared it, B's own maybeShowExifSuggestion
    # would race and could mask the bug.
    _seed_exif_photos(live_server, [anchor])
    _set_api_key()

    db = live_server["db"]

    held_routes = []

    def _hold_reverse_geocode(route):
        held_routes.append(route)

    page.route("**/api/places/reverse-geocode**", _hold_reverse_geocode)

    page.goto(f"{live_server['url']}/browse")
    page.locator(".grid-card").first.wait_for(state="visible")

    page.locator(f".grid-card[data-id='{anchor}']").click()
    _wait_for_detail_loaded(page)

    for _ in range(50):
        if held_routes:
            break
        page.wait_for_timeout(100)
    assert held_routes, "reverse-geocode fetch was never issued"

    # With the fetch still held, save a free-text location for the anchor.
    saved_text = "the meadow behind the cabin"
    inp = page.locator("#locationInput")
    inp.wait_for(state="visible")
    inp.fill(saved_text)
    inp.press("Enter")

    expect(page.locator("#locationFilled")).to_be_visible()
    expect(page.locator("#locationFilled .filled-place")).to_have_text(saved_text)
    expect(page.locator("#locationEmpty")).to_be_hidden()
    # renderLocationFilled scrubs the suggestion when the filled state
    # renders — so before we release the pending fetch, the suggestion is
    # already clean.
    assert page.evaluate(
        "() => document.getElementById('locationExifSuggestion').dataset.photoId || ''"
    ) == ""

    # Release the held reverse-geocode response. Without the fix, the
    # completion path repaints the suggestion because _detailPhotoId and
    # the selection are still A.
    held_routes[0].fulfill(
        status=200,
        content_type="application/json",
        body=json.dumps({
            "place_id": _CANNED_PLACE_ID,
            "summary": _CANNED_DETAILS["name"],
            "lat": _CANNED_DETAILS["lat"],
            "lng": _CANNED_DETAILS["lng"],
            "cached": False,
        }),
    )
    page.wait_for_timeout(300)

    # The suggestion element must stay scrubbed even before we enter batch
    # mode — no stale owner id, no stale Accept HTML.
    assert page.evaluate(
        "() => document.getElementById('locationExifSuggestion').innerHTML"
    ) == ""
    assert page.evaluate(
        "() => document.getElementById('locationExifSuggestion').dataset.photoId || ''"
    ) == ""

    # Cmd-click B into the selection. renderBatchInspector passes
    # preserveExifSuggestion; if the fix didn't hold, the preserved
    # (stale) suggestion for A would become visible here.
    page.locator(f".grid-card[data-id='{other_b}']").click(modifiers=["Meta"])
    page.wait_for_function("() => selectedPhotos.size === 2")

    expect(page.locator("#locationExifSuggestion")).to_be_hidden()
    assert page.evaluate(
        "() => document.getElementById('locationExifSuggestion').innerHTML"
    ) == ""
    assert page.evaluate(
        "() => document.getElementById('locationExifSuggestion').dataset.photoId || ''"
    ) == ""

    # A keeps the free-text location it just saved…
    row_a = db.conn.execute(
        "SELECT k.name FROM photo_keywords pk "
        "JOIN keywords k ON k.id = pk.keyword_id "
        "WHERE pk.photo_id = ? AND k.type = 'location'",
        (anchor,),
    ).fetchone()
    assert row_a is not None, "anchor should keep its saved location"
    assert row_a[0] == saved_text, (
        f"anchor's location should still be '{saved_text}', got {row_a[0]!r}"
    )
    # …and B never gained one from a resurrected Accept line.
    row_b = db.conn.execute(
        "SELECT 1 FROM photo_keywords pk "
        "JOIN keywords k ON k.id = pk.keyword_id "
        "WHERE pk.photo_id = ? AND k.type = 'location'",
        (other_b,),
    ).fetchone()
    assert row_b is None, "other photo should not have gained a location"


def test_exif_suggestion_cleared_by_close_detail(
    live_server, page, monkeypatch
):
    """Codex P2 (PR #1097): closing the detail panel used to leave the EXIF
    suggestion element populated (hidden along with its parent, but with the
    original owner's data-photo-id and Accept button still in the DOM). A
    later batch that included that owner — e.g. Select All / Ctrl+A on a view
    that still contains the closed anchor — would satisfy
    renderLocationEmpty's owner-in-selection check and resurrect the anchor's
    Accept line for the entire selection. Clicking Accept would then apply
    the closed anchor's GPS-derived place to every selected photo.

    Sequence: open A (suggestion appears) → closeDetail (× button) →
    Select All (batch inspector opens; selection includes A). The suggestion
    must stay hidden and dropped from the DOM.
    """
    photo_ids = live_server["data"]["photos"][:3]
    anchor, other_b, other_c = photo_ids
    _seed_exif_photos(live_server, [anchor])
    _seed_reverse_geocode_cache(
        live_server, 40.785091, -73.968285, _CANNED_PLACE_ID, _CANNED_DETAILS,
    )
    _set_api_key()

    import places
    monkeypatch.setattr(
        places, "place_details", lambda pid, key: _CANNED_DETAILS,
    )

    db = live_server["db"]

    page.goto(f"{live_server['url']}/browse")
    page.locator(".grid-card").first.wait_for(state="visible")

    page.locator(f".grid-card[data-id='{anchor}']").click()
    _wait_for_detail_loaded(page)
    accept = page.locator("#locationExifSuggestion button.accept-btn")
    expect(accept).to_be_visible()
    assert page.evaluate(
        "() => document.getElementById('locationExifSuggestion').dataset.photoId"
    ) == str(anchor)

    # Close the detail panel via the × button, which drives the real
    # closeDetail() path (Escape would also call clearSelection and empty
    # selectedPhotos, masking the bug).
    page.locator(".detail-close").click()
    page.wait_for_function("() => window.selectedPhotoId == null")

    # Select All — includes the closed anchor, so the batch inspector opens
    # with A in ids. Without the fix, keepSugg would be true and A's Accept
    # line would flash back into the batch panel.
    page.evaluate("() => selectAllMatchingPhotos()")
    page.wait_for_function("() => selectedPhotos.size >= 3")

    expect(page.locator("#locationExifSuggestion")).to_be_hidden()
    assert page.evaluate(
        "() => document.getElementById('locationExifSuggestion').innerHTML"
    ) == ""
    assert page.evaluate(
        "() => document.getElementById('locationExifSuggestion').dataset.photoId || ''"
    ) == ""

    # No photo in the batch may have gained a location.
    for pid in (anchor, other_b, other_c):
        row = db.conn.execute(
            "SELECT 1 FROM photo_keywords pk "
            "JOIN keywords k ON k.id = pk.keyword_id "
            "WHERE pk.photo_id = ? AND k.type = 'location'",
            (pid,),
        ).fetchone()
        assert row is None, f"photo {pid} should not have gained a location"


def test_exif_suggestion_cleared_by_anchor_drop_then_select_all(
    live_server, page, monkeypatch
):
    """Codex P2 (PR #1097): Cmd/Ctrl-click that drops the focused anchor out
    of a non-empty selection bypasses closeDetail. selectPhoto hides the
    detail panel and nulls selectedPhotoId, but the previous scrub only ran
    inside closeDetail — so #locationExifSuggestion kept its data-photo-id
    and Accept button. A later Select All that still contained the dropped
    anchor would then satisfy renderLocationEmpty's owner-in-selection check
    and resurrect the anchor's Accept line for the whole batch, letting one
    click apply the dropped anchor's GPS-derived place to every selected
    photo.

    Sequence: open A (suggestion appears) → Cmd-click B (batch mode, A still
    the focused anchor, suggestion preserved) → Cmd-click A to drop the
    anchor (selectedPhotos={B}, focus dropped, detail panel hidden, but
    closeDetail is not called) → Select All / Ctrl+A (batch inspector opens
    with A back in the selection). The suggestion must stay hidden and
    dropped from the DOM, and no photo in the batch may gain a location.
    """
    photo_ids = live_server["data"]["photos"][:3]
    anchor, other_b, other_c = photo_ids
    _seed_exif_photos(live_server, [anchor])
    _seed_reverse_geocode_cache(
        live_server, 40.785091, -73.968285, _CANNED_PLACE_ID, _CANNED_DETAILS,
    )
    _set_api_key()

    import places
    monkeypatch.setattr(
        places, "place_details", lambda pid, key: _CANNED_DETAILS,
    )

    db = live_server["db"]

    page.goto(f"{live_server['url']}/browse")
    page.locator(".grid-card").first.wait_for(state="visible")

    page.locator(f".grid-card[data-id='{anchor}']").click()
    _wait_for_detail_loaded(page)
    accept = page.locator("#locationExifSuggestion button.accept-btn")
    expect(accept).to_be_visible()
    assert page.evaluate(
        "() => document.getElementById('locationExifSuggestion').dataset.photoId"
    ) == str(anchor)

    # Grow to batch {A, B} — A is still the focused anchor, so the suggestion
    # is preserved by design.
    page.locator(f".grid-card[data-id='{other_b}']").click(modifiers=["Meta"])
    page.wait_for_function(
        "(id) => selectedPhotos.size === 2 && selectedPhotos.has(id)",
        arg=anchor,
    )
    expect(accept).to_be_visible()

    # Drop the anchor out of the selection via Cmd-click. selectPhoto's
    # metaKey branch fires — not closeDetail — so this is the code path the
    # fix targets. After this, selectedPhotos={B} but the suggestion element
    # is what we care about: it must not carry A's data-photo-id/innerHTML
    # into the next batch that includes A.
    page.locator(f".grid-card[data-id='{anchor}']").click(modifiers=["Meta"])
    page.wait_for_function(
        "(id) => selectedPhotos.size === 1 && !selectedPhotos.has(id) && "
        "window.selectedPhotoId == null",
        arg=anchor,
    )

    # Select All — includes the dropped anchor, so the batch inspector opens
    # with A back in ids. Without the fix, keepSugg would be true and A's
    # Accept line would flash back into the batch panel.
    page.evaluate("() => selectAllMatchingPhotos()")
    page.wait_for_function("() => selectedPhotos.size >= 3")

    expect(page.locator("#locationExifSuggestion")).to_be_hidden()
    assert page.evaluate(
        "() => document.getElementById('locationExifSuggestion').innerHTML"
    ) == ""
    assert page.evaluate(
        "() => document.getElementById('locationExifSuggestion').dataset.photoId || ''"
    ) == ""

    # No photo in the batch may have gained a location.
    for pid in (anchor, other_b, other_c):
        row = db.conn.execute(
            "SELECT 1 FROM photo_keywords pk "
            "JOIN keywords k ON k.id = pk.keyword_id "
            "WHERE pk.photo_id = ? AND k.type = 'location'",
            (pid,),
        ).fetchone()
        assert row is None, f"photo {pid} should not have gained a location"


def test_exif_suggestion_cleared_by_batch_clear_then_select_all(
    live_server, page, monkeypatch
):
    """Codex P2 (PR #1097): the batch-bar Clear button invokes clearSelection()
    directly, which used to bypass every EXIF-suggestion scrub path. With
    A's Accept line visible, Cmd-click B, click the batch-bar Clear button,
    then Select All on a view that still contains A: renderLocationEmpty's
    owner-in-selection check would find A in the new batch and re-show the
    old #locationExifSuggestion. Accept would then apply A's place_id to
    every selected photo.

    Sequence: open A (suggestion appears) → Cmd-click B (batch mode, A still
    focused anchor, suggestion preserved) → clearSelection() (mirrors the
    batch-bar Clear onclick) → Select All / Ctrl+A. The suggestion must
    stay hidden and dropped from the DOM, and no photo may gain a location.
    """
    photo_ids = live_server["data"]["photos"][:3]
    anchor, other_b, other_c = photo_ids
    _seed_exif_photos(live_server, [anchor])
    _seed_reverse_geocode_cache(
        live_server, 40.785091, -73.968285, _CANNED_PLACE_ID, _CANNED_DETAILS,
    )
    _set_api_key()

    import places
    monkeypatch.setattr(
        places, "place_details", lambda pid, key: _CANNED_DETAILS,
    )

    db = live_server["db"]

    page.goto(f"{live_server['url']}/browse")
    page.locator(".grid-card").first.wait_for(state="visible")

    page.locator(f".grid-card[data-id='{anchor}']").click()
    _wait_for_detail_loaded(page)
    accept = page.locator("#locationExifSuggestion button.accept-btn")
    expect(accept).to_be_visible()
    assert page.evaluate(
        "() => document.getElementById('locationExifSuggestion').dataset.photoId"
    ) == str(anchor)

    # Grow to batch {A, B} — A is still the focused anchor.
    page.locator(f".grid-card[data-id='{other_b}']").click(modifiers=["Meta"])
    page.wait_for_function(
        "(id) => selectedPhotos.size === 2 && selectedPhotos.has(id)",
        arg=anchor,
    )
    expect(accept).to_be_visible()

    # Click the batch-bar Clear button by invoking the same function it does.
    # This is the code path the fix targets — clearSelection() bypasses
    # closeDetail and the selectPhoto deselect scrub, so it needs its own.
    page.evaluate("() => clearSelection()")
    page.wait_for_function(
        "() => selectedPhotos.size === 0 && window.selectedPhotoId == null"
    )

    # Immediately after Clear, the suggestion must already be scrubbed —
    # without the fix it would still be tagged with A's id, hidden inside
    # the empty container waiting to resurrect.
    assert page.evaluate(
        "() => document.getElementById('locationExifSuggestion').dataset.photoId || ''"
    ) == ""
    assert page.evaluate(
        "() => document.getElementById('locationExifSuggestion').innerHTML"
    ) == ""

    # Select All — includes the previously-open anchor. Without the fix,
    # keepSugg would be true (owner A is in the new ids) and A's Accept line
    # would flash back into the batch panel.
    page.evaluate("() => selectAllMatchingPhotos()")
    page.wait_for_function("() => selectedPhotos.size >= 3")

    expect(page.locator("#locationExifSuggestion")).to_be_hidden()
    assert page.evaluate(
        "() => document.getElementById('locationExifSuggestion').innerHTML"
    ) == ""
    assert page.evaluate(
        "() => document.getElementById('locationExifSuggestion').dataset.photoId || ''"
    ) == ""

    # No photo in the batch may have gained a location.
    for pid in (anchor, other_b, other_c):
        row = db.conn.execute(
            "SELECT 1 FROM photo_keywords pk "
            "JOIN keywords k ON k.id = pk.keyword_id "
            "WHERE pk.photo_id = ? AND k.type = 'location'",
            (pid,),
        ).fetchone()
        assert row is None, f"photo {pid} should not have gained a location"


def test_exif_suggestion_bails_when_slow_geocode_resolves_after_close_detail(
    live_server, page, monkeypatch
):
    """Codex P2 (PR #1097, 17:04Z follow-up): closeDetail() scrubs the
    #locationExifSuggestion DOM but used to leave window._detailPhotoId
    pointing at the departed photo. maybeShowExifSuggestion's post-await
    guards checked `_detailPhotoId === requestPhotoId` and
    `_locationApplyPhotoIds().indexOf(requestPhotoId) !== -1` — after
    close + Select All, both were still satisfied (the pointer never
    moved and the departed photo was back in the selection), so a slow
    reverse-geocode for A would repaint A's Accept line into the batch
    inspector. Clicking Accept would then apply A's EXIF place to every
    selected photo.

    Sequence: open A (reverse-geocode intercepted and held) → close the
    detail panel via the × button → Select All (batch inspector opens
    with A back in the selection) → release the held response. The
    suggestion must stay hidden and no photo may gain a location.
    """
    photo_ids = live_server["data"]["photos"][:3]
    anchor, other_b, other_c = photo_ids
    # Only the anchor has GPS. B and C have no lat/lng, so their own
    # maybeShowExifSuggestion calls bail immediately and can't race with
    # ours (which would mask the bug).
    _seed_exif_photos(live_server, [anchor])
    _set_api_key()

    import places
    monkeypatch.setattr(
        places, "place_details", lambda pid, key: _CANNED_DETAILS,
    )

    db = live_server["db"]

    held_routes = []

    def _hold_reverse_geocode(route):
        held_routes.append(route)

    page.route("**/api/places/reverse-geocode**", _hold_reverse_geocode)

    page.goto(f"{live_server['url']}/browse")
    page.locator(".grid-card").first.wait_for(state="visible")

    page.locator(f".grid-card[data-id='{anchor}']").click()
    _wait_for_detail_loaded(page)

    # maybeShowExifSuggestion awaits _cfgPromise before fetching, so wait
    # for the browser to actually issue the reverse-geocode request.
    for _ in range(50):
        if held_routes:
            break
        page.wait_for_timeout(100)
    assert held_routes, "reverse-geocode fetch was never issued"

    # Close the detail panel via the × button — the real closeDetail()
    # path. Escape would also clear selectedPhotos, hiding the bug.
    page.locator(".detail-close").click()
    page.wait_for_function("() => window.selectedPhotoId == null")
    # The scrub null-ed the ambient owner pointer synchronously.
    assert page.evaluate("() => window._detailPhotoId") is None

    # Select All — the departed anchor is back in the selection.
    page.evaluate("() => selectAllMatchingPhotos()")
    page.wait_for_function("() => selectedPhotos.size >= 3")

    # Now release the held reverse-geocode response. Without the fix, the
    # completion path would repaint A's Accept line into the batch panel
    # (both async guards still pass because _detailPhotoId still equals A
    # and A is now in the selection).
    held_routes[0].fulfill(
        status=200,
        content_type="application/json",
        body=json.dumps({
            "place_id": _CANNED_PLACE_ID,
            "summary": _CANNED_DETAILS["name"],
            "lat": _CANNED_DETAILS["lat"],
            "lng": _CANNED_DETAILS["lng"],
            "cached": False,
        }),
    )
    # Give the async completion a chance to run. If it were going to paint,
    # it'd happen synchronously after the fetch/JSON awaits resolve.
    page.wait_for_timeout(300)

    expect(page.locator("#locationExifSuggestion")).to_be_hidden()
    assert page.evaluate(
        "() => document.getElementById('locationExifSuggestion').innerHTML"
    ) == ""
    assert page.evaluate(
        "() => document.getElementById('locationExifSuggestion').dataset.photoId || ''"
    ) == ""

    # And no photo in the batch may have gained a location.
    for pid in (anchor, other_b, other_c):
        row = db.conn.execute(
            "SELECT 1 FROM photo_keywords pk "
            "JOIN keywords k ON k.id = pk.keyword_id "
            "WHERE pk.photo_id = ? AND k.type = 'location'",
            (pid,),
        ).fetchone()
        assert row is None, f"photo {pid} should not have gained a location"


def test_exif_suggestion_bails_when_slow_geocode_resolves_after_batch_clear(
    live_server, page, monkeypatch
):
    """Codex P2 (PR #1097, 17:04Z follow-up): the batch-bar Clear button
    calls clearSelection(), which scrubs the DOM but used to leave
    window._detailPhotoId pointing at the departed anchor. A pending
    reverse-geocode for A then resolves after the user hits Clear and
    Select All (which brings A back into the batch); the async guards
    still pass, so A's Accept line repaints into the batch inspector and
    Accept posts A's EXIF place to every selected photo.

    Sequence: open A (reverse-geocode held) → Cmd-click B (batch mode) →
    clearSelection() (mirrors the batch-bar Clear onclick) → Select All →
    release the held response. The suggestion must stay hidden and no
    photo may gain a location.
    """
    photo_ids = live_server["data"]["photos"][:3]
    anchor, other_b, other_c = photo_ids
    _seed_exif_photos(live_server, [anchor])
    _set_api_key()

    import places
    monkeypatch.setattr(
        places, "place_details", lambda pid, key: _CANNED_DETAILS,
    )

    db = live_server["db"]

    held_routes = []

    def _hold_reverse_geocode(route):
        held_routes.append(route)

    page.route("**/api/places/reverse-geocode**", _hold_reverse_geocode)

    page.goto(f"{live_server['url']}/browse")
    page.locator(".grid-card").first.wait_for(state="visible")

    page.locator(f".grid-card[data-id='{anchor}']").click()
    _wait_for_detail_loaded(page)

    for _ in range(50):
        if held_routes:
            break
        page.wait_for_timeout(100)
    assert held_routes, "reverse-geocode fetch was never issued"

    # Grow to batch {A, B} — A is still the focused anchor.
    page.locator(f".grid-card[data-id='{other_b}']").click(modifiers=["Meta"])
    page.wait_for_function(
        "(id) => selectedPhotos.size === 2 && selectedPhotos.has(id)",
        arg=anchor,
    )

    # Click the batch-bar Clear button by invoking the same function it
    # does. This is the code path the fix targets — clearSelection()
    # bypasses closeDetail entirely, so it needs its own scrubs.
    page.evaluate("() => clearSelection()")
    page.wait_for_function(
        "() => selectedPhotos.size === 0 && window.selectedPhotoId == null"
    )
    assert page.evaluate("() => window._detailPhotoId") is None

    # Select All — A is back in the batch.
    page.evaluate("() => selectAllMatchingPhotos()")
    page.wait_for_function("() => selectedPhotos.size >= 3")

    # Release the held reverse-geocode response.
    held_routes[0].fulfill(
        status=200,
        content_type="application/json",
        body=json.dumps({
            "place_id": _CANNED_PLACE_ID,
            "summary": _CANNED_DETAILS["name"],
            "lat": _CANNED_DETAILS["lat"],
            "lng": _CANNED_DETAILS["lng"],
            "cached": False,
        }),
    )
    page.wait_for_timeout(300)

    expect(page.locator("#locationExifSuggestion")).to_be_hidden()
    assert page.evaluate(
        "() => document.getElementById('locationExifSuggestion').innerHTML"
    ) == ""
    assert page.evaluate(
        "() => document.getElementById('locationExifSuggestion').dataset.photoId || ''"
    ) == ""

    for pid in (anchor, other_b, other_c):
        row = db.conn.execute(
            "SELECT 1 FROM photo_keywords pk "
            "JOIN keywords k ON k.id = pk.keyword_id "
            "WHERE pk.photo_id = ? AND k.type = 'location'",
            (pid,),
        ).fetchone()
        assert row is None, f"photo {pid} should not have gained a location"


def test_exif_suggestion_cleared_by_loaddetail_before_select_all(
    live_server, page, monkeypatch
):
    """Codex P2 (PR #1097, 17:23Z): loadDetail(B) hides the panel and awaits
    /api/photos/B before renderDetail runs — the maybeShowExifSuggestion call
    that scrubs #locationExifSuggestion only fires once that response
    resolves. If the user clicks B and quickly hits Select All while B's
    fetch is still in flight, the previous photo's stale suggestion (with
    data-photo-id=A) is still in the DOM. renderBatchInspector's
    preserveExifSuggestion check sees A in the Select All ids and
    resurrects A's Accept line for the whole batch — clicking it applies
    A's EXIF place to every selected photo.

    Sequence: open A (suggestion appears, tagged for A) → intercept and
    hold /api/photos/B → click B (loadDetail starts, fetch held) → Select
    All (batch inspector opens with A back in ids). The suggestion must
    stay hidden and no photo may gain a location.
    """
    photo_ids = live_server["data"]["photos"][:3]
    anchor, other_b, other_c = photo_ids
    # Only the anchor has GPS. B and C have no lat/lng, so their own
    # maybeShowExifSuggestion calls bail immediately and can't race with
    # ours (which would mask the bug).
    _seed_exif_photos(live_server, [anchor])
    _seed_reverse_geocode_cache(
        live_server, 40.785091, -73.968285, _CANNED_PLACE_ID, _CANNED_DETAILS,
    )
    _set_api_key()

    import places
    monkeypatch.setattr(
        places, "place_details", lambda pid, key: _CANNED_DETAILS,
    )

    db = live_server["db"]

    page.goto(f"{live_server['url']}/browse")
    page.locator(".grid-card").first.wait_for(state="visible")

    page.locator(f".grid-card[data-id='{anchor}']").click()
    _wait_for_detail_loaded(page)
    accept = page.locator("#locationExifSuggestion button.accept-btn")
    expect(accept).to_be_visible()
    assert page.evaluate(
        "() => document.getElementById('locationExifSuggestion').dataset.photoId"
    ) == str(anchor)

    # Hold B's detail fetch. This is the code path the fix targets — the
    # DOM scrubs inside renderDetail (via renderLocationEmpty /
    # renderLocationFilled) only run once /api/photos/B resolves, so any
    # Select All that lands during the await used to see A's stale
    # data-photo-id and resurrect A's Accept line for the batch.
    held_routes = []

    def _hold_photo_b(route):
        held_routes.append(route)

    page.route(f"**/api/photos/{other_b}", _hold_photo_b)

    page.locator(f".grid-card[data-id='{other_b}']").click()

    # Wait for loadDetail(B) to have actually started and issued the held
    # fetch. Once it has, the pre-await scrub in loadDetail must have run
    # synchronously — the ambient owner pointer is nulled and the DOM tag
    # is dropped even though B's response hasn't arrived yet.
    for _ in range(50):
        if held_routes:
            break
        page.wait_for_timeout(100)
    assert held_routes, "/api/photos/B fetch was never issued"
    assert page.evaluate("() => window._detailPhotoId") is None
    assert page.evaluate(
        "() => document.getElementById('locationExifSuggestion').dataset.photoId || ''"
    ) == ""

    # Select All while B's detail is still stalled. Without the pre-await
    # scrub, keepSugg would be true (A's data-photo-id still present, A in
    # the Select All ids) and A's Accept line would flash back into the
    # batch inspector.
    page.evaluate("() => selectAllMatchingPhotos()")
    page.wait_for_function("() => selectedPhotos.size >= 3")

    expect(page.locator("#locationExifSuggestion")).to_be_hidden()
    assert page.evaluate(
        "() => document.getElementById('locationExifSuggestion').innerHTML"
    ) == ""
    assert page.evaluate(
        "() => document.getElementById('locationExifSuggestion').dataset.photoId || ''"
    ) == ""

    # Release B's held detail response so nothing dangles at teardown.
    # loadDetail's post-await guard (`selectedPhotos.size <= 1`) will drop
    # this render anyway now that Select All has promoted the selection,
    # but we still want the network layer to complete cleanly.
    held_routes[0].fulfill(
        status=200,
        content_type="application/json",
        body=json.dumps({
            "id": other_b,
            "filename": f"photo_{other_b}.jpg",
            "rating": 0,
            "flag": None,
            "color_label": None,
            "keywords": [],
            "xmp_keywords": [],
            "xmp_exists": False,
            "metadata": {},
            "location": None,
            "latitude": None,
            "longitude": None,
        }),
    )
    page.wait_for_timeout(200)

    # After the response drains, the suggestion must still be gone — the
    # post-await guard in loadDetail must skip renderDetail while the
    # batch selection is active.
    expect(page.locator("#locationExifSuggestion")).to_be_hidden()
    assert page.evaluate(
        "() => document.getElementById('locationExifSuggestion').dataset.photoId || ''"
    ) == ""

    # No photo in the batch may have gained a location.
    for pid in (anchor, other_b, other_c):
        row = db.conn.execute(
            "SELECT 1 FROM photo_keywords pk "
            "JOIN keywords k ON k.id = pk.keyword_id "
            "WHERE pk.photo_id = ? AND k.type = 'location'",
            (pid,),
        ).fetchone()
        assert row is None, f"photo {pid} should not have gained a location"


def test_freetext_location_batches_selection_and_refreshes_smart_collection(
    live_server, page
):
    """Typing a free-text location applies to the active selection."""
    photo_ids = live_server["data"]["photos"][:3]
    _seed_exif_photos(live_server, photo_ids)

    db = live_server["db"]
    collection_id = next(
        c["id"]
        for c in db.get_collections()
        if c["name"] == "GPS Without Location Keyword"
    )
    assert db.count_collection_photos(collection_id) == 3

    page.goto(f"{live_server['url']}/browse")
    page.locator(".grid-card").first.wait_for(state="visible")
    page.evaluate("(collectionId) => filterByCollection(collectionId)", collection_id)
    page.wait_for_function(
        "(collectionId) => activeCollectionId === collectionId && photos.length === 3",
        arg=collection_id,
    )

    page.locator(f".grid-card[data-id='{photo_ids[0]}']").click()
    _wait_for_detail_loaded(page)
    page.locator(f".grid-card[data-id='{photo_ids[1]}']").click(modifiers=["Meta"])
    page.locator(f".grid-card[data-id='{photo_ids[2]}']").click(modifiers=["Meta"])
    page.wait_for_function("() => selectedPhotos.size === 3")

    inp = page.locator("#locationInput")
    inp.fill("the meadow")
    inp.press("Enter")
    page.wait_for_function("() => activeCollectionId !== null && photos.length === 0")

    for photo_id in photo_ids:
        row = db.conn.execute(
            "SELECT k.name FROM photo_keywords pk "
            "JOIN keywords k ON k.id = pk.keyword_id "
            "WHERE pk.photo_id = ? AND k.type = 'location'",
            (photo_id,),
        ).fetchone()
        assert row["name"] == "the meadow"
        expect(page.locator(f".grid-card[data-id='{photo_id}']")).to_have_count(0)
    assert db.count_collection_photos(collection_id) == 0
