import base64

from playwright.sync_api import expect

_PNG_1X1 = (
    "iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAQAAAC1HAwCAAAAC0lEQVR42mP8"
    "/x8AAwMCAO+/p9sAAAAASUVORK5CYII="
)


def _mock_pipeline_rapid_review(page, *, results=None, state_ok=True, apply_photos=None, save_payloads=None):
    image_body = base64.b64decode(_PNG_1X1)
    if results is None:
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
    if state_ok:
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
    else:
        page.route("**/api/pipeline/group/state", lambda route: route.fulfill(status=500, json={"error": "boom"}))
    page.route(
        "**/api/pipeline/group/apply",
        lambda route: route.fulfill(
            json={
                "ok": True,
                "photos": apply_photos
                or {
                    "1": {"flag": "rejected", "has_species_keyword": False},
                    "2": {"flag": "rejected", "has_species_keyword": False},
                    "3": {"flag": "none", "has_species_keyword": False},
                },
            }
        ),
    )
    def save_cache(route):
        if save_payloads is not None:
            save_payloads.append(route.request.post_data_json)
        route.fulfill(json={"ok": True})

    page.route("**/api/pipeline/save-cache", save_cache)
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


def test_rapid_review_keeps_apply_disabled_when_state_load_fails(live_server, page):
    _mock_pipeline_rapid_review(page, state_ok=False)

    page.goto(f"{live_server['url']}/pipeline/rapid-review")

    expect(page.locator("#filename")).to_have_text("a.jpg")
    expect(page.locator("#applyBtn")).to_be_disabled()
    expect(page.locator("#burstSubtitle")).to_contain_text("Failed to load burst state")

    page.keyboard.press("ArrowDown")
    expect(page.locator("#filename")).to_have_text("a.jpg")
    expect(page.locator("#rejectCount")).to_have_text("0")


def test_rapid_review_rewrites_all_burst_labels_before_saving_cache(live_server, page):
    save_payloads = []
    results = {
        "photos": [
            {"id": 1, "filename": "a.jpg", "label": "REVIEW", "quality_composite": 0.3, "subject_tenengrad": 10},
            {"id": 2, "filename": "b.jpg", "label": "REJECT", "quality_composite": 0.6, "subject_tenengrad": 20},
            {"id": 3, "filename": "c.jpg", "label": "KEEP", "quality_composite": 0.9, "subject_tenengrad": 30},
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
        "summary": {"keep_count": 1, "review_count": 1, "reject_count": 1},
    }
    _mock_pipeline_rapid_review(
        page,
        results=results,
        apply_photos={
            "1": {"flag": "none", "has_species_keyword": False},
            "2": {"flag": "none", "has_species_keyword": False},
            "3": {"flag": "none", "has_species_keyword": False},
        },
        save_payloads=save_payloads,
    )

    page.goto(f"{live_server['url']}/pipeline/rapid-review")
    expect(page.locator("#applyBtn")).to_be_enabled()

    with page.expect_response("**/api/pipeline/save-cache"):
        page.locator("#applyBtn").click()

    assert save_payloads
    saved_photos = {p["id"]: p for p in save_payloads[-1]["photos"]}
    assert saved_photos[1]["label"] == "REVIEW"
    assert saved_photos[2]["label"] == "REVIEW"
    assert saved_photos[3]["label"] == "REVIEW"
    assert saved_photos[2]["flag"] == "none"
    assert saved_photos[3]["flag"] == "none"
