"""Right-click file reveals target the individual duplicate copy."""

import json

import pytest
from PIL import Image
from playwright.sync_api import expect

from e2e.test_duplicates_bulk_decide import _seed_scan_with_buckets


@pytest.mark.parametrize("resolved", [False, True])
def test_duplicate_image_reveal_targets_clicked_copy(live_server, page, tmp_path, resolved):
    db = live_server["db"]
    _, winners, losers = _seed_scan_with_buckets(
        db, str(tmp_path / "originals"), str(tmp_path / "copies"), n_groups=1,
    )
    if resolved:
        db.update_photo_flag(losers[0], "rejected")
    for pid in winners + losers:
        Image.new("RGB", (100, 100), color="green").save(
            f"{live_server['app'].config['THUMB_CACHE_DIR']}/{pid}.jpg"
        )

    requests = []

    def reveal(route):
        requests.append(route.request.post_data_json)
        route.fulfill(status=200, content_type="application/json", body='{"ok": true}')

    page.route("**/api/files/reveal", reveal)
    page.goto(f"{live_server['url']}/duplicates")
    if resolved:
        page.locator("#resolvedToggle").click()

    for pid in winners + losers:
        card = page.locator(f'.dup-card[data-photo-id="{pid}"]')
        card.locator("img.thumb").click(button="right")
        menu = page.locator(".vireo-ctx-menu")
        expect(menu).to_be_visible()
        with page.expect_response("**/api/files/reveal"):
            menu.locator(".vireo-ctx-item", has_text="Reveal in").click()
        expect(menu).to_be_hidden()
        assert requests[-1] == {"photo_id": pid, "scope": "duplicates"}
        expect(card).to_be_visible()


@pytest.mark.parametrize("status", [200, 500])
def test_duplicate_reveal_failure_is_visible(live_server, page, tmp_path, status):
    _, winners, _ = _seed_scan_with_buckets(
        live_server["db"], str(tmp_path / "originals"), str(tmp_path / "copies"), n_groups=1,
    )
    page.route("**/api/files/reveal", lambda route: route.fulfill(
        status=status, content_type="application/json",
        body=json.dumps({"ok": False, "reason": "file unavailable", "error": "file unavailable"}),
    ))
    page.goto(f"{live_server['url']}/duplicates")
    card = page.locator(f'.dup-card[data-photo-id="{winners[0]}"]')
    card.click(button="right")
    page.locator(".vireo-ctx-item", has_text="Reveal in").click()
    expect(page.get_by_role("alert").filter(has_text="Reveal failed: file unavailable")).to_be_visible()
