"""Discover legacy folders on Process and queue a scoped Reclassify."""

import pytest
from playwright.sync_api import expect
from services import detection_store


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
            for photo in live_server["data"]["photos"]:
                detection_store.write_detection_batch(db, photo, "megadetector-v6", [{
                    "box": {"x": 0.1, "y": 0.1, "w": 0.5, "h": 0.5},
                    "confidence": 0.95, "category": "animal",
                }], runtime_fingerprint="raw-detector-runtime")
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
        assert live_server["db"].conn.execute(
            "SELECT COUNT(*) FROM predictions WHERE labels_fingerprint = 'legacy'",
        ).fetchone()[0] == 5
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


def test_legacy_nudge_includes_unclassified_detector_outputs(live_server, page):
    db = live_server["db"]
    db.conn.execute("DELETE FROM predictions")
    db.conn.commit()
    photos = live_server["data"]["photos"]
    db.detections.save(photos[0], [{
        "box": {"x": 0.1, "y": 0.1, "w": 0.5, "h": 0.5},
        "confidence": 0.01, "category": "animal",
    }], "megadetector-v6")
    db.model_runs.record_detector_run(photos[3], "megadetector-v6", 0)
    db.commit()
    page.goto(f'{live_server["url"]}/pipeline')
    card = page.locator("#legacyDetectionsCard")
    expect(card).to_be_visible()
    expect(card.locator("strong")).to_have_text("2 folders have legacy detections.")
    card.locator("summary").click()
    expect(card.locator("li")).to_have_text([
        "/photos/park — 1 photo", "/photos/yard — 1 photo",
    ])


@pytest.mark.parametrize("refresh", ["empty", "remaining", "error"])
def test_legacy_retry_waits_for_fresh_folder_state(live_server, page, refresh):
    _legacy_library(live_server)
    queued = []
    held = []

    def enqueue(route):
        queued.append(route.request.post_data_json)
        route.fulfill(json={"job_id": "pipeline-legacy-test"})

    page.route("**/api/jobs/pipeline", enqueue)
    page.route("**/api/jobs/pipeline-legacy-test/stream", lambda route: route.fulfill(
        content_type="text/event-stream",
        body='event: complete\ndata: {"status":"completed"}\n\n',
    ))
    page.goto(f'{live_server["url"]}/pipeline')
    expect(page.locator("#legacyDetectionsCard")).to_be_visible()
    expect(page.locator(".model-checkbox:checked")).to_have_count(1)
    # Hold the completion refresh independently of the successful queue/stream.
    page.route("**/api/pipeline/legacy-detections", lambda route: held.append(route))
    page.locator("#btnReclassifyLegacy").click()
    status = page.locator("#legacyDetectionsStatus")
    expect(status).to_contain_text("Refreshing affected folders")
    button = page.locator("#btnReclassifyLegacy")
    expect(button).to_be_disabled()
    # The in-flight guard must also reject direct invocations of the action.
    page.evaluate("reclassifyLegacyFolders()")
    assert len(queued) == 1
    assert len(held) == 1

    if refresh == "error":
        held[0].fulfill(status=503, json={"error": "temporary discovery failure"})
        expect(status).to_contain_text("Could not refresh affected folders")
        expect(button).to_be_disabled()
        page.evaluate("reclassifyLegacyFolders()")
        assert len(queued) == 1
    elif refresh == "empty":
        held[0].fulfill(json={"folders": []})
        expect(page.locator("#legacyDetectionsCard")).to_be_hidden()
        page.evaluate("reclassifyLegacyFolders()")
        assert len(queued) == 1
    else:
        held[0].fulfill(json={"folders": [{
            "id": live_server["data"]["folders"][1], "path": "/photos/yard", "photo_count": 1,
        }]})
        expect(button).to_be_enabled()
        expect(page.locator("#legacyDetectionsTitle")).to_have_text("1 folder has legacy detections.")
        # An intentional retry must use only the newly returned scope.
        button.click()
        expect(status).to_contain_text("Refreshing affected folders")
        assert len(queued) == 2
        assert queued[1]["folder_ids"] == [live_server["data"]["folders"][1]]
        held[1].fulfill(json={"folders": []})
        expect(page.locator("#legacyDetectionsCard")).to_be_hidden()
