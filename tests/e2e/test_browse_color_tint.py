"""The browse grid tints a card with its color label.

A color label is a property of the photo, so it has to be visible on the card
without depending on the optional `color_label` badge field — which is off by
default, so a labelled photo used to look identical to an unlabelled one.
"""

import re

from playwright.sync_api import expect


def _rgba(value):
    """Computed color -> (r, g, b, a) with channels on 0-1, rounded to 2dp.

    Accepts both serializations Chromium uses: `rgb()`/`rgba()` with 0-255
    channels, and the `color(srgb ...)` form that color-mix() produces.
    """
    nums = [float(n) for n in re.findall(r"[\d.]+", value)]
    assert len(nums) in (3, 4), value
    if not value.startswith("color("):
        nums[:3] = [n / 255 for n in nums[:3]]
    if len(nums) == 3:
        nums.append(1.0)
    return tuple(round(n, 2) for n in nums)


def test_card_carries_its_saved_color_label(live_server, page):
    """A label saved before the page loads reaches the card via the async fetch."""
    url = live_server["url"]
    photo_id = live_server["data"]["photos"][0]

    assert page.request.post(
        f"{url}/api/photos/{photo_id}/color_label", data={"color": "purple"}
    ).ok

    page.goto(f"{url}/browse")
    card = page.locator(f'.grid-card[data-id="{photo_id}"]')
    card.wait_for(state="visible")
    expect(card).to_have_attribute("data-color-label", "purple")

    # Assert the properties that actually implement the tint. Border color is
    # useless here: _navbar.html pins .grid-card border-color with !important,
    # which is why the tint uses `outline` at all, so a border assertion would
    # pass with every tint rule deleted.
    outline = card.evaluate(
        "el => { const s = getComputedStyle(el);"
        " return [s.outlineColor, s.outlineStyle, s.outlineWidth]; }"
    )
    assert _rgba(outline[0]) == (0.61, 0.35, 0.71, 1.0), outline
    assert outline[1:] == ["solid", "2px"], outline
    strip = card.locator(".grid-card-info").evaluate(
        "el => getComputedStyle(el).backgroundColor"
    )
    # color-mix() serializes as `color(srgb r g b / a)` with 0-1 channels in
    # Chromium, not `rgba()`, so compare the parsed channels rather than a
    # serialization this assertion has no business pinning.
    assert _rgba(strip) == (0.61, 0.35, 0.71, 0.16), strip

    # Unlabelled cards stay untinted.
    other = page.locator(f'.grid-card:not([data-id="{photo_id}"])').first
    assert other.get_attribute("data-color-label") is None
    assert other.evaluate("el => getComputedStyle(el).outlineStyle") == "none"


def test_setting_and_clearing_a_color_updates_the_card_live(live_server, page):
    """The detail panel's color buttons repaint the card without a reload."""
    url = live_server["url"]
    photo_id = live_server["data"]["photos"][0]

    page.goto(f"{url}/browse")
    card = page.locator(f'.grid-card[data-id="{photo_id}"]')
    card.wait_for(state="visible")
    card.click()

    green = page.locator('#detailColors [data-color="green"]')
    expect(green).to_be_visible()
    green.click()
    expect(card).to_have_attribute("data-color-label", "green")
    assert _rgba(card.evaluate("el => getComputedStyle(el).outlineColor")) == (
        0.18, 0.8, 0.44, 1.0
    )

    # Clicking the active color clears it, and the tint goes with it.
    green.click()
    expect(card).not_to_have_attribute("data-color-label", "green")
    assert card.get_attribute("data-color-label") is None
    assert card.evaluate("el => getComputedStyle(el).outlineStyle") == "none"


def test_offline_cards_keep_their_tint_across_a_full_rerender(live_server, page):
    """renderPhotoCard's offline branch builds its own HTML and must carry the tint.

    Regression: the attribute was added only after the offline early return, so
    an offline photo lost its tint on any full renderGrid() (Select all, a sort
    change) and nothing re-fetched labels to put it back.
    """
    url = live_server["url"]
    photo_id = live_server["data"]["photos"][0]
    assert page.request.post(
        f"{url}/api/photos/{photo_id}/color_label", data={"color": "blue"}
    ).ok

    page.goto(f"{url}/browse")
    card = page.locator(f'.grid-card[data-id="{photo_id}"]')
    card.wait_for(state="visible")
    expect(card).to_have_attribute("data-color-label", "blue")

    page.evaluate(
        """(pid) => {
            const p = photos.find(x => x.id === pid);
            if (!p) throw new Error('seed photo missing from page state');
            p.folder_status = 'missing';
            renderGrid();
        }""",
        photo_id,
    )

    offline = page.locator(f'.grid-card.offline[data-id="{photo_id}"]')
    expect(offline).to_have_attribute("data-color-label", "blue")
    assert _rgba(offline.evaluate("el => getComputedStyle(el).outlineColor")) == (
        0.2, 0.6, 0.86, 1.0
    )


def test_a_label_edited_mid_fetch_is_not_restored_by_the_stale_response(
    live_server, page
):
    """An in-flight color-labels GET must not speak for a photo edited since.

    Regression: fetchColorLabels checked the edit generation only when clearing
    stale entries, then merged every value from the response unconditionally —
    so a slow bootstrap GET put the pre-edit label back until reload.
    """
    url = live_server["url"]
    photo_id = live_server["data"]["photos"][0]
    other_id = live_server["data"]["photos"][1]
    assert page.request.post(
        f"{url}/api/photos/{photo_id}/color_label", data={"color": "red"}
    ).ok
    assert page.request.post(
        f"{url}/api/photos/{other_id}/color_label", data={"color": "green"}
    ).ok

    page.goto(f"{url}/browse")
    page.locator(f'.grid-card[data-id="{photo_id}"]').wait_for(state="visible")
    expect(page.locator(f'.grid-card[data-id="{photo_id}"]')).to_have_attribute(
        "data-color-label", "red"
    )

    # Gate the next color-labels GET at Vireo.api.json — vireo-api.js binds
    # window.fetch at load time, so patching window.fetch would not intercept.
    page.evaluate(
        """() => {
            const orig = window.Vireo.api.json;
            let release;
            const gate = new Promise((r) => { release = r; });
            window.__releaseColorLabels = release;
            window.Vireo.api.json = function (u) {
                const args = arguments;
                if (String(u).includes('/api/photos/color_labels')) {
                    // Issue the request now, deliver the answer later. Gating
                    // the request instead would let the clear reach the server
                    // first, so the response would agree with the edit and the
                    // test would pass with no fix at all.
                    const inflight = orig.apply(window.Vireo.api, args);
                    return Promise.all([inflight, gate]).then((r) => r[0]);
                }
                return orig.apply(window.Vireo.api, args);
            };
        }"""
    )

    # Start the stalled fetch, then clear one label while it is in flight.
    page.evaluate(
        "(ids) => { window.__pending = fetchColorLabels(ids); }",
        [photo_id, other_id],
    )
    page.evaluate("(pid) => setColorLabelFor(pid, null)", photo_id)
    expect(page.locator(f'.grid-card[data-id="{photo_id}"]')).not_to_have_attribute(
        "data-color-label", "red"
    )

    # Release the now-stale response and let it merge.
    page.evaluate("() => window.__releaseColorLabels()")
    page.evaluate("() => window.__pending")
    page.evaluate("() => refreshGridCards(photos.map(p => p.id))")

    # The edited photo keeps the user's clear; the untouched one still hydrates.
    assert (
        page.locator(f'.grid-card[data-id="{photo_id}"]').get_attribute(
            "data-color-label"
        )
        is None
    )
    expect(page.locator(f'.grid-card[data-id="{other_id}"]')).to_have_attribute(
        "data-color-label", "green"
    )


def test_a_fetch_started_during_a_pending_write_cannot_undo_it(live_server, page):
    """The generations tie when a fetch starts while the label POST is in flight.

    Regression: the per-id stamp was written once, before the POST. A fetch
    starting after that stamp captured the *same* generation, so `stamp > gen`
    was false — and its response, read from the server before the write
    committed, put the old color back. Re-stamping when the write lands puts
    such a fetch strictly behind it.
    """
    url = live_server["url"]
    photo_id = live_server["data"]["photos"][0]
    assert page.request.post(
        f"{url}/api/photos/{photo_id}/color_label", data={"color": "red"}
    ).ok

    page.goto(f"{url}/browse")
    card = page.locator(f'.grid-card[data-id="{photo_id}"]')
    card.wait_for(state="visible")
    expect(card).to_have_attribute("data-color-label", "red")

    # Stub the color-labels GET to answer with the pre-write server truth,
    # delivered only when released. Stubbing the body rather than racing the
    # real endpoint is what makes the interleaving deterministic — the point
    # under test is what the client does with a response it already knows how
    # to produce, not whether SQLite commits in time.
    page.evaluate(
        """(pid) => {
            const orig = window.Vireo.api.json;
            let release;
            const gate = new Promise((r) => { release = r; });
            window.__releaseColorLabels = release;
            window.Vireo.api.json = function (u) {
                if (String(u).includes('/api/photos/color_labels')) {
                    const stale = {}; stale[pid] = 'red';
                    return gate.then(() => stale);
                }
                return orig.apply(window.Vireo.api, arguments);
            };
        }""",
        photo_id,
    )

    # Write, then start a fetch while that write is still in flight.
    page.evaluate(
        "(pid) => { window.__write = setColorLabelFor(pid, null); }", photo_id
    )
    page.evaluate("(pid) => { window.__pending = fetchColorLabels([pid]); }", photo_id)
    page.evaluate("() => window.__write")
    expect(card).not_to_have_attribute("data-color-label", "red")

    # Let the stale response land on the completed write.
    page.evaluate("() => window.__releaseColorLabels()")
    page.evaluate("() => window.__pending")
    page.evaluate("(pid) => refreshGridCards([pid])", photo_id)

    assert card.get_attribute("data-color-label") is None
    assert page.evaluate("(pid) => colorLabels[pid] || null", photo_id) is None
