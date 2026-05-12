import base64

from playwright.sync_api import expect

_PNG_1X1 = (
    "iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAQAAAC1HAwCAAAAC0lEQVR42mP8"
    "/x8AAwMCAO+/p9sAAAAASUVORK5CYII="
)


def _mock_pipeline_rapid_review(page):
    image_body = base64.b64decode(_PNG_1X1)
    results = {
        "photos": [
            {"id": 1, "filename": "a.jpg", "label": "REVIEW", "quality_composite": 0.3, "subject_tenengrad": 10},
            {"id": 2, "filename": "b.jpg", "label": "REVIEW", "quality_composite": 0.6, "subject_tenengrad": 20},
            {"id": 3, "filename": "c.jpg", "label": "REVIEW", "quality_composite": 0.9, "subject_tenengrad": 30},
        ],
        "encounters": [
            {
                "photo_ids": [1, 2, 3],
                "photo_count": 3,
                "burst_count": 1,
                "species": ["Test bird"],
                "bursts": [{"photo_ids": [1, 2, 3]}],
            }
        ],
        "summary": {"keep_count": 0, "review_count": 3, "reject_count": 0},
    }

    page.route("**/api/pipeline/results", lambda route: route.fulfill(json=results))
    page.route(
        "**/api/pipeline/group/state",
        lambda route: route.fulfill(
            json={
                "photos": {
                    "1": {"flag": "none", "has_species_keyword": False},
                    "2": {"flag": "none", "has_species_keyword": False},
                    "3": {"flag": "none", "has_species_keyword": False},
                },
                "species_kid": None,
            }
        ),
    )
    page.route(
        "**/api/pipeline/group/apply",
        lambda route: route.fulfill(
            json={
                "ok": True,
                "photos": {
                    "1": {"flag": "rejected", "has_species_keyword": False},
                    "2": {"flag": "rejected", "has_species_keyword": False},
                    "3": {"flag": "none", "has_species_keyword": False},
                },
            }
        ),
    )
    page.route("**/api/pipeline/save-cache", lambda route: route.fulfill(json={"ok": True}))
    page.route(
        "**/thumbnails/*.jpg",
        lambda route: route.fulfill(body=image_body, content_type="image/png"),
    )
    page.route(
        "**/photos/*/preview?*",
        lambda route: route.fulfill(body=image_body, content_type="image/png"),
    )


def test_rapid_review_decision_keys_advance_through_queue(live_server, page):
    _mock_pipeline_rapid_review(page)

    page.goto(f"{live_server['url']}/pipeline/rapid-review")

    expect(page.locator("#filename")).to_have_text("a.jpg")

    page.keyboard.press("ArrowDown")
    expect(page.locator("#filename")).to_have_text("b.jpg")
    expect(page.locator("#rejectCount")).to_have_text("1")

    page.keyboard.press("ArrowRight")
    expect(page.locator("#filename")).to_have_text("c.jpg")
    expect(page.locator("#metricReviewed")).to_have_text("2/3")

    page.keyboard.press("ArrowLeft")
    expect(page.locator("#filename")).to_have_text("b.jpg")
    expect(page.locator("#metricReviewed")).to_have_text("1/3")
