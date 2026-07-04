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
