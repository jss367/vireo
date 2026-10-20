"""Provisional ranking stays inspectable without enabling automatic flags."""

import json

from playwright.sync_api import expect


def _open_provisional_batch(live_server, page):
    ids = live_server["data"]["photos"][:2]
    cards = [{"id": pid, "filename": f"frame{index}.jpg", "rank": index, "role": "review",
              "label": "REVIEW", "quality_pct": 70, "reasons": ["Absolute focus has not been validated"]}
             for index, pid in enumerate(ids, 1)]
    payload = {"photo_ids": ids, "count": 2, "best_photo_id": None, "ranked_candidate_id": ids[0],
               "ranking_status": "provisional", "suggested_reject_ids": [], "cards": cards,
               "keep_review_message": "Absolute focus and animal-subject validity have not been validated; review this frame."}
    page.route("**/api/photos/best-batch", lambda route: route.fulfill(json=payload))
    page.add_init_script(f"sessionStorage.setItem('vireo.bestBatchIds', {json.dumps(json.dumps(ids))});")
    page.goto(f"{live_server['url']}/best-batch")
    expect(page.locator("#bestContent")).to_be_visible()
    expect(page.locator("#focusNote")).to_contain_text("have not been validated")
    expect(page.locator("#rankList .role").first).to_have_text("#1 Needs review")
    expect(page.locator("#rejectBtn")).to_be_disabled()
    expect(page.get_by_role("button", name="Flag Inspected Photo")).to_be_enabled()
    return ids


def test_best_batch_provisional_ranking_allows_only_deliberate_inspected_flag(live_server, page):
    ids = _open_provisional_batch(live_server, page)
    page.locator(f'#rankList [data-photo-id="{ids[1]}"]').click()
    expect(page.locator("#inspectName")).to_have_text("frame2.jpg")
    with page.expect_response("**/api/batch/flag") as response:
        page.get_by_role("button", name="Flag Inspected Photo").click()
    assert response.value.ok
    assert live_server["db"].photos.get(ids[0])["flag"] == "none"
    assert live_server["db"].photos.get(ids[1])["flag"] == "flagged"
    expect(page.locator("#inspectRole")).to_have_text("#2 Kept by you")
    expect(page.locator(f'#rankList [data-photo-id="{ids[1]}"] .role')).to_have_text("#2 Kept by you")
    expect(page.locator(f'#focusGrid [data-photo-id="{ids[1]}"] .role')).to_have_text("#2 Kept by you")
    expect(page.locator("#rankList .role").first).to_have_text("#1 Needs review")
    expect(page.locator("#rejectBtn")).to_be_disabled()


def test_flag_response_keeps_the_current_inspection_and_updates_the_requested_frame(live_server, page):
    ids = _open_provisional_batch(live_server, page)
    pending = []
    page.route("**/api/batch/flag", lambda route: pending.append(route))
    with page.expect_request("**/api/batch/flag"):
        page.get_by_role("button", name="Flag Inspected Photo").click()
    page.locator(f'#rankList [data-photo-id="{ids[1]}"]').click()
    expect(page.locator("#inspectName")).to_have_text("frame2.jpg")
    assert len(pending) == 1
    assert pending[0].request.post_data_json["photo_ids"] == [ids[0]]
    pending[0].continue_()
    expect(page.locator("#rankList .role").first).to_have_text("#1 Kept by you")
    expect(page.locator("#inspectRole")).to_have_text("#2 Needs review")
    expect(page.locator("#inspectName")).to_have_text("frame2.jpg")
    assert live_server["db"].photos.get(ids[0])["flag"] == "flagged"
    assert live_server["db"].photos.get(ids[1])["flag"] == "none"


def test_failed_flag_preserves_the_review_status(live_server, page):
    ids = _open_provisional_batch(live_server, page)
    page.route("**/api/batch/flag", lambda route: route.fulfill(
        status=500, json={"error": "The flag could not be saved"}))
    with page.expect_response("**/api/batch/flag"):
        page.get_by_role("button", name="Flag Inspected Photo").click()
    expect(page.locator("#inspectRole")).to_have_text("#1 Needs review")
    expect(page.locator("#rankList .role").first).to_have_text("#1 Needs review")
    assert live_server["db"].photos.get(ids[0])["flag"] == "none"
