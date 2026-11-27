"""Process Review shows the saved review without waiting for the readiness counts."""

import io

from PIL import Image
from playwright.sync_api import expect

READINESS = {
    "state": "computable", "total_photos": 4, "with_masks": 4, "mask_target_photos": 4,
    "with_embeddings": 0, "embedding_target_photos": 4, "with_eye_keypoints": 0,
    "with_eye_keypoint_attempts": 0, "eye_keypoint_target_photos": 0,
    "with_predictions": 4, "prediction_target_photos": 4,
    "missing_required": [], "enhancing_missing": ["embeddings"],
}


def _saved_review():
    predictions = [{"species": "Black Phoebe", "models": [{"model": "Test classifier", "confidence": .9}]}]
    photos = [
        {"id": pid, "filename": f"bird-{pid}.NEF", "label": "REVIEW", "flag": "none",
         "quality_composite": .5, "species_top5": [["Black Phoebe", .9, "Test classifier"]]}
        for pid in (1, 2, 3, 4)
    ]
    ids = [photo["id"] for photo in photos]
    return {
        "photos": photos,
        "encounters": [{
            "photo_ids": ids, "photo_count": len(ids), "burst_count": 1,
            "species": ["Black Phoebe"], "species_predictions": predictions,
            "bursts": [{"photo_ids": ids, "species_predictions": predictions}],
        }],
        "summary": {"total_photos": len(ids), "encounter_count": 1, "burst_count": 1,
                    "review_count": len(ids), "keep_count": 0, "reject_count": 0},
    }


def _open_review(page, live_server, results):
    """Open Process Review with the readiness request held; return what was asked for."""
    asked = {"results": 0, "readiness": []}

    def page_init(route):
        if "include=readiness" in route.request.url:
            asked["readiness"].append(route)
            return
        assert "include=results" in route.request.url
        asked["results"] += 1
        route.fulfill(json={"workspace_id": 1, "results": results, "workspace_overrides": {}})

    thumbnail = io.BytesIO()
    Image.new("RGB", (160, 107), "green").save(thumbnail, "JPEG")
    page.route("**/api/pipeline/page-init*", page_init)
    page.route("**/thumbnails/*", lambda route: route.fulfill(
        body=thumbnail.getvalue(), content_type="image/jpeg"))
    page.goto(f"{live_server['url']}/pipeline/review", wait_until="domcontentloaded")
    return asked


def test_review_photos_appear_before_readiness_and_the_banner_follows(live_server, page):
    asked = _open_review(page, live_server, _saved_review())

    expect(page.locator("#encountersContainer .photo-card")).to_have_count(4)
    expect(page.locator("#degradedBanner")).to_be_hidden()
    assert asked["results"] == 1
    assert len(asked["readiness"]) == 1

    asked["readiness"][0].fulfill(json={"workspace_id": 1, "review_readiness": {**READINESS, "state": "ready"}})

    page.get_by_text("Review details", exact=True).click()
    expect(page.locator("#degradedBanner")).to_be_visible()
    expect(page.locator("#degradedBannerText")).to_contain_text("computed without embeddings")
    expect(page.locator("#encountersContainer .photo-card")).to_have_count(4)


def test_review_without_saved_results_waits_for_readiness_to_explain_why(live_server, page):
    asked = _open_review(page, live_server, None)

    # The results answer has arrived (it set the workspace id) and the empty
    # state still waits for the counts it is made of.
    page.wait_for_function("() => pipelineReviewWorkspaceId === 1")
    expect(page.locator("#emptyState")).to_be_hidden()
    assert len(asked["readiness"]) == 1

    asked["readiness"][0].fulfill(json={"workspace_id": 1, "review_readiness": READINESS})

    expect(page.locator("#emptyStateHeading")).to_have_text("Ready to compute results")
    expect(page.locator("#readinessStages")).to_contain_text("0 / 4")


def test_review_reports_a_failed_readiness_check_and_keeps_the_photos(live_server, page):
    asked = _open_review(page, live_server, _saved_review())
    expect(page.locator("#encountersContainer .photo-card")).to_have_count(4)

    asked["readiness"][0].fulfill(status=500, json={"error": "invented readiness failure"})

    expect(page.locator('#toastContainer [data-type="error"]')).to_contain_text(
        "Could not check which analysis steps are missing")
    expect(page.locator("#encountersContainer .photo-card")).to_have_count(4)


def test_saved_photos_can_open_while_review_results_refresh(live_server, page):
    held = []
    photo_id = live_server["data"]["photos"][0]
    page.route("**/api/pipeline/page-init?include=results", lambda route: held.append(route))
    page.route("**/api/pipeline/review-preview", lambda route: route.fulfill(
        json={"photos": [{"id": photo_id, "label": "bird.NEF"}]}))
    page.goto(live_server["url"] + "/pipeline/review", wait_until="domcontentloaded")
    link = page.get_by_role("link", name="bird.NEF")
    expect(link).to_be_visible()
    expect(link).to_have_attribute("href", f"/browse?photo_id={photo_id}")
    assert held
    held[0].fulfill(json={"workspace_id": 1, "results": _saved_review(), "workspace_overrides": {}})
    expect(page.locator("#encountersContainer .photo-card")).to_have_count(4)
    expect(page.locator("#loadingPhotos")).to_be_hidden()
