"""Cull page: manual and already-applied decisions survive a recompute.

Tuning a slider re-runs the analysis and rebuilds every card from the fresh
labels. The decisions the user made by hand (and the flags they already
applied) must come through that rebuild untouched, and the page must say how
many it kept rather than letting a preserved decision look like agreement.
"""

import re

from playwright.sync_api import expect


def _results(photo_overrides=None):
    photos = [
        {"id": 1, "filename": "enc1-a.jpg", "label": "KEEP", "quality_composite": 0.91},
        {"id": 2, "filename": "enc1-b.jpg", "label": "REVIEW", "quality_composite": 0.67},
        {"id": 3, "filename": "enc2-a.jpg", "label": "KEEP", "quality_composite": 0.88},
    ]
    for photo in photos:
        photo.update((photo_overrides or {}).get(photo["id"], {}))
    return {
        "photos": photos,
        "encounters": [
            {
                "photo_ids": [1, 2],
                "photo_count": 2,
                "burst_count": 1,
                "species": ["Test bird"],
                "bursts": [{"photo_ids": [1, 2]}],
                "time_range": ["2024-03-10T08:00:00", "2024-03-10T08:01:00"],
            },
            {
                "photo_ids": [3],
                "photo_count": 1,
                "burst_count": 1,
                "species": ["Test bird"],
                "bursts": [{"photo_ids": [3]}],
                "time_range": ["2024-03-10T08:05:00", "2024-03-10T08:05:00"],
            },
        ],
        "summary": {"keep_count": 2, "review_count": 1, "reject_count": 0},
    }


def _open_cull(page, live_server, results):
    page.route(
        "**/api/pipeline/page-init",
        lambda route: route.fulfill(json={"results": results}),
    )
    page.goto(f"{live_server['url']}/cull")
    expect(page.locator(".cull-card")).to_have_count(3)


def _card(page, photo_id):
    return page.locator('.cull-card[data-photo-id="%d"]' % photo_id)


def _cycle(page, photo_id):
    _card(page, photo_id).locator(".cull-card-action").click()


def _nudge_scoring_slider(page, reflow_results):
    """Drive a real slider so the page re-runs the analysis."""
    page.route(
        "**/api/pipeline/reflow",
        lambda route: route.fulfill(json=reflow_results),
    )
    page.click("#cullSettingsBtn")
    page.locator("#slRejectComposite").focus()
    page.keyboard.press("ArrowRight")


def test_manual_decision_survives_recompute(live_server, page):
    _open_cull(page, live_server, _results())

    # Photo 1 is suggested KEEP; call it Review by hand.
    _cycle(page, 1)
    expect(_card(page, 1)).to_have_class(re.compile(r"\breview\b"))
    expect(_card(page, 1).locator(".cull-card-source")).to_have_text("Pinned")
    expect(page.locator(".summary-stat", has_text="Pinned by you")).to_contain_text("1")

    # Recompute with a payload that promotes photo 2 to KEEP: the fresh label
    # lands on photo 2, the hand decision on photo 1 stays put.
    _nudge_scoring_slider(page, _results({2: {"label": "KEEP"}}))

    expect(_card(page, 2)).to_have_class(re.compile(r"\bkeep\b"))
    expect(_card(page, 1)).to_have_class(re.compile(r"\breview\b"))
    expect(_card(page, 1).locator(".cull-card-source")).to_have_text("Pinned")
    expect(page.locator("#cullStatus")).to_contain_text("1 pinned decision kept")


def test_pinned_decisions_can_be_released(live_server, page):
    _open_cull(page, live_server, _results())

    _cycle(page, 1)
    _cycle(page, 3)
    expect(page.locator(".summary-link", has_text="Clear 2 pinned")).to_be_visible()

    # Releasing one card puts that photo back on the suggestion.
    _card(page, 1).locator(".cull-card-source").click()
    expect(_card(page, 1)).to_have_class(re.compile(r"\bkeep\b"))
    expect(_card(page, 1).locator(".cull-card-source")).to_have_count(0)
    expect(page.locator(".summary-link", has_text="Clear 1 pinned")).to_be_visible()

    # Clearing the rest restores every suggestion.
    page.locator(".summary-link", has_text="Clear 1 pinned").click()
    expect(_card(page, 3)).to_have_class(re.compile(r"\bkeep\b"))
    expect(page.locator(".cull-card-source")).to_have_count(0)
    expect(page.locator(".summary-stat", has_text="Pinned by you")).to_have_count(0)


def test_applied_flags_win_over_fresh_suggestion(live_server, page):
    _open_cull(page, live_server, _results({1: {"flag": "rejected"}}))

    # Photo 1 was rejected by an earlier apply, so that decision shows even
    # though this run's label says KEEP.
    expect(_card(page, 1)).to_have_class(re.compile(r"\breject\b"))
    expect(_card(page, 1).locator(".cull-card-source")).to_have_text("Applied")
    expect(page.locator(".summary-stat", has_text="Already applied")).to_contain_text("1")

    page.locator(".summary-link", has_text="Ignore 1 saved flags").click()
    expect(_card(page, 1)).to_have_class(re.compile(r"\bkeep\b"))
    expect(page.locator(".cull-card-source")).to_have_count(0)

    page.locator(".summary-link", has_text="Honor 1 saved flags").click()
    expect(_card(page, 1)).to_have_class(re.compile(r"\breject\b"))


def test_applied_flags_survive_recompute(live_server, page):
    _open_cull(page, live_server, _results({1: {"flag": "rejected"}}))
    expect(_card(page, 1)).to_have_class(re.compile(r"\breject\b"))

    _nudge_scoring_slider(page, _results({1: {"flag": "rejected"}, 2: {"label": "KEEP"}}))

    expect(_card(page, 2)).to_have_class(re.compile(r"\bkeep\b"))
    expect(_card(page, 1)).to_have_class(re.compile(r"\breject\b"))
    expect(_card(page, 1).locator(".cull-card-source")).to_have_text("Applied")


def test_apply_clears_the_flag_behind_a_review_decision(live_server, page):
    _open_cull(page, live_server, _results({1: {"flag": "flagged"}}))

    posted = []

    def capture(route):
        posted.append(route.request.post_data_json)
        route.fulfill(json={"ok": True, "keepers": 1, "rejects": 0, "cleared": 1})

    page.route("**/api/culling/apply", capture)
    page.on("dialog", lambda dialog: dialog.accept())

    # Photo 1 carries a saved "flagged"; move it to Review by hand.
    _cycle(page, 1)
    expect(_card(page, 1)).to_have_class(re.compile(r"\breview\b"))

    page.click("#applyBtn")
    expect(page.locator("#cullStatus")).to_contain_text("cleared")

    assert posted and posted[0]["unflag"] == [1]
    assert 1 not in posted[0]["keepers"]
    # The card keeps showing the decision (no flag value means "undecided",
    # which is indistinguishable from "never looked at"), but it is saved now,
    # so it no longer blocks undo as unsaved work.
    expect(_card(page, 1)).to_have_class(re.compile(r"\breview\b"))
    expect(_card(page, 1).locator(".cull-card-source")).to_have_text("Pinned")
    assert page.evaluate("cullDirty") is False


def test_apply_turns_keep_and_reject_pins_into_saved_flags(live_server, page):
    _open_cull(page, live_server, _results())

    page.route(
        "**/api/culling/apply",
        lambda route: route.fulfill(json={"ok": True, "keepers": 2, "rejects": 1, "cleared": 0}),
    )
    page.on("dialog", lambda dialog: dialog.accept())

    # Photo 2 is suggested REVIEW; call it Reject (review -> reject).
    _cycle(page, 2)
    expect(_card(page, 2)).to_have_class(re.compile(r"\breject\b"))
    expect(_card(page, 2).locator(".cull-card-source")).to_have_text("Pinned")

    page.click("#applyBtn")
    expect(page.locator("#cullStatus")).to_contain_text("Applied!")

    # The decision is a saved flag now, not a session override.
    expect(_card(page, 2)).to_have_class(re.compile(r"\breject\b"))
    expect(_card(page, 2).locator(".cull-card-source")).to_have_text("Applied")
    expect(page.locator(".summary-stat", has_text="Pinned by you")).to_have_count(0)
    assert page.evaluate("cullDirty") is False
