"""Discover legacy folders on Process and queue a scoped Reclassify."""

import pytest
from playwright.sync_api import expect


def _legacy_library(live_server):
    db = live_server["db"]
    db.conn.execute("UPDATE predictions SET labels_fingerprint = 'legacy'")
    db.conn.commit()


@pytest.mark.parametrize("outcome", ["failed", "completed"])
def test_legacy_nudge_queues_reclassify(live_server, page, outcome):
    _legacy_library(live_server)
    requests = []

    def enqueue(route):
        requests.append(route.request.post_data_json)
        if outcome == "completed":
            db = live_server["db"]
            db.conn.execute("UPDATE predictions SET labels_fingerprint = 'regenerated'")
            db.conn.commit()
        route.fulfill(json={"job_id": "pipeline-legacy-test"})

    page.route("**/api/jobs/pipeline", enqueue)
    page.route("**/api/jobs/pipeline-legacy-test/stream", lambda route: route.fulfill(
        content_type="text/event-stream",
        body=f'event: complete\ndata: {{"status":"{outcome}"}}\n\n',
    ))
    page.goto(f'{live_server["url"]}/pipeline')
    card = page.locator("#legacyDetectionsCard")
    expect(card).to_be_visible()
    expect(card.locator("strong")).to_have_text("2 folders have legacy detections.")
    card.locator("summary").click()
    expect(card.locator("li")).to_have_text([
        "/photos/park — 3 photos", "/photos/yard — 2 photos",
    ])
    # Model loading is asynchronous; queue only once its picker is populated.
    expect(page.locator(".model-checkbox:checked")).to_have_count(1)
    page.locator("#btnReclassifyLegacy").click()
    if outcome == "failed":
        expect(page.locator("#legacyDetectionsStatus")).to_contain_text("Reclassify failed")
        expect(page.locator("#legacyDetectionsJobs")).to_be_visible()
        expect(page.locator("#btnReclassifyLegacy")).to_be_enabled()
    else:
        expect(card).to_be_hidden()
    assert len(requests) == 1
    body = requests[0]
    assert sorted(body["folder_ids"]) == sorted(live_server["data"]["folders"])
    assert body["reclassify"] is True
    assert body["model_ids"] == ["bioclip-2"]
    assert body["skip_classify"] is False
    assert body["skip_extract_masks"] and body["skip_eye_keypoints"] and body["skip_regroup"]
    assert body["miss_enabled"] is False and body["review_mode"] is None
    assert "collection_id" not in body and "source" not in body


def test_legacy_nudge_blocks_missing_classification_setup(live_server, page):
    _legacy_library(live_server)
    requests = []
    page.route("**/api/jobs/pipeline", lambda route: requests.append(route.request))
    page.route("**/api/pipeline/plan", lambda route: route.fulfill(json={"stages": {
        "Classify": {"state": "blocked", "summary": "Select a species list in Settings."},
    }}))
    page.goto(f'{live_server["url"]}/pipeline')
    expect(page.locator(".model-checkbox:checked")).to_have_count(1)
    expect(page.locator("#legacyDetectionsCard")).to_be_visible()
    page.locator("#btnReclassifyLegacy").click()
    expect(page.locator("#legacyDetectionsStatus")).to_have_text("Select a species list in Settings.")
    expect(page.locator("#btnReclassifyLegacy")).to_be_enabled()
    assert requests == []


def test_current_library_hides_legacy_nudge(live_server, page):
    with page.expect_response("**/api/pipeline/legacy-detections"):
        page.goto(f'{live_server["url"]}/pipeline')
    expect(page.locator("#legacyDetectionsCard")).to_be_hidden()
