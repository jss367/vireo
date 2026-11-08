"""Cull page: a reject says what it lost to, and orphaned rejects come back.

Rejecting a photo records it in favor of the kept photo it was compared
against (here the best kept frame of its encounter), the card and the
Apply payload both say so, and a saved reject whose keeper has since been
deleted or rejected is listed in a banner so it can go back to undecided.
"""

import json

from playwright.sync_api import expect


def _results():
    return {
        "photos": [
            {"id": 1, "filename": "enc1-a.jpg", "label": "KEEP", "quality_composite": 0.91,
             "focus_evidence": {"basis": "eye", "measurement": 1840.2, "measurement_name": "Tenengrad"}},
            {"id": 2, "filename": "enc1-b.jpg", "label": "REVIEW", "quality_composite": 0.67,
             "focus_evidence": {"basis": "eye", "measurement": 610.4, "measurement_name": "Tenengrad"}},
            {"id": 3, "filename": "enc2-a.jpg", "label": "KEEP", "quality_composite": 0.88},
        ],
        "encounters": [
            {"photo_ids": [1, 2], "photo_count": 2, "burst_count": 1, "species": ["Test bird"],
             "bursts": [{"photo_ids": [1, 2]}],
             "time_range": ["2024-03-10T08:00:00", "2024-03-10T08:01:00"]},
            {"photo_ids": [3], "photo_count": 1, "burst_count": 1, "species": ["Test bird"],
             "bursts": [{"photo_ids": [3]}],
             "time_range": ["2024-03-10T08:05:00", "2024-03-10T08:05:00"]},
        ],
        "summary": {"keep_count": 2, "review_count": 1, "reject_count": 0},
    }


def _open_cull(page, live_server, results, orphans=()):
    page.route("**/api/pipeline/page-init", lambda route: route.fulfill(json={"results": results}))
    page.route("**/api/culling/orphans",
               lambda route: route.fulfill(json={"orphans": list(orphans), "count": len(orphans)}))
    page.goto(f"{live_server['url']}/cull")
    expect(page.locator(".cull-card")).to_have_count(3)


def _card(page, photo_id):
    return page.locator('.cull-card[data-photo-id="%d"]' % photo_id)


def test_reject_is_recorded_in_favor_of_the_kept_photo(live_server, page):
    _open_cull(page, live_server, _results())
    page.on("dialog", lambda dialog: dialog.accept())
    applied = []
    page.route("**/api/culling/apply", lambda route: (
        applied.append(json.loads(route.request.post_data)),
        route.fulfill(json={"ok": True, "keepers": 1, "rejects": 1, "cleared": 0, "rejections": 1}),
    ))

    # Photo 1 is a suggested favorite; reject its neighbor 2 by hand.
    _card(page, 2).get_by_role("button", name="Reject", exact=True).click()
    keeper = _card(page, 2).locator(".cull-keeper")
    expect(keeper).to_contain_text("Will be rejected instead of enc1-a.jpg")
    expect(keeper).to_have_attribute("title", "Instead of enc1-a.jpg · eye Tenengrad 610 vs 1840 (same encounter)")

    page.click("#applyBtn")
    expect(page.locator("#cullStatus")).to_contain_text("Applied!")
    assert applied[0]["rejections"] == [{
        "photo_id": 2, "keeper_photo_id": 1,
        "comparison": {"basis": "eye", "measurement_name": "Tenengrad", "photo": 610, "keeper": 1840,
                       "scope": "encounter"},
    }]
    # Saved now: the card keeps saying what it lost to, in the past tense.
    expect(_card(page, 2).locator(".cull-keeper")).to_have_text("rejected instead of enc1-a.jpg")
    expect(_card(page, 2).locator(".cull-card-source")).to_have_text("Applied")


def test_orphaned_rejects_are_listed_and_can_go_back_to_undecided(live_server, page):
    orphan = {"photo_id": 2, "filename": "enc1-b.jpg", "keeper_photo_id": 1, "keeper_filename": None,
              "comparison": None, "orphan_reason": "keeper_deleted", "description": "", "at": ""}
    _open_cull(page, live_server, _results(), orphans=[orphan])
    banner = page.locator("#cullOrphans")
    expect(banner).to_be_visible()
    expect(banner).to_contain_text("1 rejected photo lost the photo it was rejected for.")

    applied = []
    page.route("**/api/culling/apply", lambda route: (
        applied.append(json.loads(route.request.post_data)),
        route.fulfill(json={"ok": True, "keepers": 0, "rejects": 0, "cleared": 1, "rejections": 0}),
    ))
    banner.get_by_role("button", name="Review them").click()
    dialog = page.locator("#cullOrphansDialog")
    expect(dialog).to_be_visible()
    expect(dialog).to_contain_text("Instead of a photo since deleted (no shared measurement) — that photo was deleted.")
    dialog.get_by_role("button", name="Keep undecided").click()
    expect(dialog).to_be_hidden()
    expect(banner).to_be_hidden()
    assert applied == [{"keepers": [], "rejects": [], "unflag": [2]}]
