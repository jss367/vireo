import base64
import re

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


def test_rapid_review_preserves_burst_override_species_on_apply_next(live_server, page):
    results = {
        "photos": [
            {"id": 1, "filename": "a.jpg", "label": "REVIEW", "quality_composite": 0.3, "subject_tenengrad": 10},
            {"id": 4, "filename": "d.jpg", "label": "REVIEW", "quality_composite": 0.8, "subject_tenengrad": 40},
        ],
        "encounters": [
            {
                "photo_ids": [1, 4],
                "photo_count": 2,
                "burst_count": 2,
                "species": ["Encounter bird"],
                "bursts": [
                    {"photo_ids": [1]},
                    {"photo_ids": [4], "species_override": {"species": "Override bird", "confirmed": True}},
                ],
            }
        ],
        "summary": {"keep_count": 0, "review_count": 2, "reject_count": 0},
    }
    _mock_pipeline_rapid_review(
        page,
        results=results,
        apply_photos={
            "1": {"flag": "none", "has_species_keyword": False},
            "4": {"flag": "none", "has_species_keyword": False},
        },
    )

    page.goto(f"{live_server['url']}/pipeline/rapid-review")
    expect(page.locator("#speciesInput")).to_have_value("Encounter bird")
    page.locator("#speciesInput").fill("New encounter bird")

    with page.expect_response("**/api/pipeline/save-cache"):
        page.locator("#applyNextBtn").click()

    expect(page.locator("#filename")).to_have_text("d.jpg")
    expect(page.locator("#speciesInput")).to_have_value("Override bird")


def test_rapid_review_default_queue_excludes_fully_confirmed_bursts(live_server, page):
    results = {
        "photos": [
            {"id": 1, "filename": "confirmed.jpg", "label": "KEEP", "flag": "flagged", "confirmed_species": "Test bird"},
            {"id": 2, "filename": "needs.jpg", "label": "REVIEW", "flag": "none"},
        ],
        "encounters": [
            {
                "photo_ids": [1, 2],
                "photo_count": 2,
                "burst_count": 2,
                "species": ["Test bird"],
                "bursts": [{"photo_ids": [1]}, {"photo_ids": [2]}],
            }
        ],
        "summary": {"keep_count": 1, "review_count": 1, "reject_count": 0},
    }
    _mock_pipeline_rapid_review(page, results=results)

    page.goto(f"{live_server['url']}/pipeline/rapid-review")

    expect(page.locator("#queueFilter")).to_have_value("needs-species")
    expect(page.locator("#queueCount")).to_contain_text("Needs species: 1 of 2 bursts")
    expect(page.locator("#filename")).to_have_text("needs.jpg")
    expect(page.locator(".burst-row")).to_have_count(1)

    page.locator("#queueFilter").select_option("all")
    expect(page.locator("#queueCount")).to_contain_text("All: 2 of 2 bursts")
    expect(page.locator(".burst-row")).to_have_count(2)


def test_rapid_review_needs_species_includes_mixed_burst(live_server, page):
    results = {
        "photos": [
            {"id": 1, "filename": "tagged.jpg", "label": "KEEP", "flag": "flagged", "confirmed_species": "Test bird"},
            {"id": 2, "filename": "untagged.jpg", "label": "REVIEW", "flag": "none"},
        ],
        "encounters": [
            {
                "photo_ids": [1, 2],
                "photo_count": 2,
                "burst_count": 1,
                "species": ["Test bird"],
                "bursts": [{"photo_ids": [1, 2]}],
            }
        ],
        "summary": {"keep_count": 1, "review_count": 1, "reject_count": 0},
    }
    _mock_pipeline_rapid_review(page, results=results)

    page.goto(f"{live_server['url']}/pipeline/rapid-review")

    expect(page.locator("#queueCount")).to_contain_text("Needs species: 1 of 1 bursts")
    expect(page.locator(".reason-chip", has_text="No species")).to_be_visible()
    expect(page.locator(".reason-chip", has_text="Already tagged")).to_be_visible()


def test_rapid_review_needs_species_ignores_rejected_untagged_photos(live_server, page):
    results = {
        "photos": [
            {"id": 1, "filename": "tagged.jpg", "label": "KEEP", "flag": "flagged", "confirmed_species": "Test bird"},
            {"id": 2, "filename": "rejected.jpg", "label": "REJECT", "flag": "rejected"},
        ],
        "encounters": [
            {
                "photo_ids": [1, 2],
                "photo_count": 2,
                "burst_count": 1,
                "species": ["Test bird"],
                "bursts": [{"photo_ids": [1, 2]}],
            }
        ],
        "summary": {"keep_count": 1, "review_count": 0, "reject_count": 1},
    }
    _mock_pipeline_rapid_review(page, results=results)

    page.goto(f"{live_server['url']}/pipeline/rapid-review")

    expect(page.locator("#queueCount")).to_contain_text("Needs species: 0 of 1 bursts")
    expect(page.locator("#burstSubtitle")).to_have_text("0 matching bursts")

    page.locator("#includeRejectedToggle").check()
    expect(page.locator("#queueCount")).to_contain_text("Needs species: 1 of 1 bursts")
    expect(page.locator("#filename")).to_have_text("tagged.jpg")


def test_rapid_review_apply_next_skips_bursts_outside_active_queue(live_server, page):
    results = {
        "photos": [
            {"id": 1, "filename": "first.jpg", "label": "REVIEW", "flag": "none"},
            {"id": 2, "filename": "done.jpg", "label": "KEEP", "flag": "flagged", "confirmed_species": "Test bird"},
            {"id": 3, "filename": "third.jpg", "label": "REVIEW", "flag": "none"},
        ],
        "encounters": [
            {
                "photo_ids": [1, 2, 3],
                "photo_count": 3,
                "burst_count": 3,
                "species": ["Test bird"],
                "bursts": [{"photo_ids": [1]}, {"photo_ids": [2]}, {"photo_ids": [3]}],
            }
        ],
        "summary": {"keep_count": 1, "review_count": 2, "reject_count": 0},
    }
    _mock_pipeline_rapid_review(page, results=results)

    page.goto(f"{live_server['url']}/pipeline/rapid-review")
    expect(page.locator("#filename")).to_have_text("first.jpg")
    page.locator("#speciesInput").fill("Test bird")
    page.keyboard.press("ArrowUp")

    with page.expect_response("**/api/pipeline/save-cache"):
        page.locator("#applyNextBtn").click()

    expect(page.locator("#filename")).to_have_text("third.jpg")
    expect(page.locator("#queueCount")).to_contain_text("Needs species: 1 of 3 bursts")


def test_rapid_review_rebases_active_session_after_apply_rebuild(live_server, page):
    results = {
        "photos": [
            {"id": 1, "filename": "first.jpg", "label": "REVIEW", "flag": "none"},
            {"id": 2, "filename": "done.jpg", "label": "KEEP", "flag": "flagged", "confirmed_species": "Test bird"},
            {"id": 3, "filename": "third.jpg", "label": "REVIEW", "flag": "none"},
        ],
        "encounters": [
            {
                "photo_ids": [1, 2, 3],
                "photo_count": 3,
                "burst_count": 3,
                "species": ["Test bird"],
                "bursts": [{"photo_ids": [1]}, {"photo_ids": [2]}, {"photo_ids": [3]}],
            }
        ],
        "summary": {"keep_count": 1, "review_count": 2, "reject_count": 0},
    }
    _mock_pipeline_rapid_review(
        page,
        results=results,
        apply_photos={"1": {"flag": "flagged", "has_species_keyword": True}},
    )

    page.goto(f"{live_server['url']}/pipeline/rapid-review")
    expect(page.locator("#applyBtn")).to_be_enabled()
    expect(page.locator("#filename")).to_have_text("first.jpg")
    page.locator("#speciesInput").fill("Test bird")
    page.keyboard.press("ArrowUp")

    page.evaluate(
        """async () => {
            const done = applyCurrent(false);
            openSession(1);
            await done;
        }"""
    )

    expect(page.locator("#burstTitle")).to_have_text("Encounter 1, Burst 3")
    expect(page.locator("#filename")).to_have_text("third.jpg")
    expect(page.locator("#queueCount")).to_contain_text("Needs species: 1 of 3 bursts - 1 of 1")


def test_rapid_review_filter_change_prompts_with_staged_decisions(live_server, page):
    results = {
        "photos": [
            {"id": 1, "filename": "needs.jpg", "label": "REVIEW", "flag": "none"},
            {"id": 2, "filename": "done.jpg", "label": "KEEP", "flag": "flagged", "confirmed_species": "Test bird"},
        ],
        "encounters": [
            {
                "photo_ids": [1, 2],
                "photo_count": 2,
                "burst_count": 2,
                "species": ["Test bird"],
                "bursts": [{"photo_ids": [1]}, {"photo_ids": [2]}],
            }
        ],
        "summary": {"keep_count": 1, "review_count": 1, "reject_count": 0},
    }
    _mock_pipeline_rapid_review(page, results=results)

    page.goto(f"{live_server['url']}/pipeline/rapid-review")
    expect(page.locator("#applyBtn")).to_be_enabled()
    page.keyboard.press("ArrowDown")
    page.locator("#queueFilter").select_option("all")

    expect(page.locator("#queuePrompt")).to_be_visible()
    page.locator('[data-queue-choice="stay"]').click()
    expect(page.locator("#queueFilter")).to_have_value("needs-species")

    page.locator("#queueFilter").select_option("all")
    expect(page.locator("#queuePrompt")).to_be_visible()
    page.locator('[data-queue-choice="discard"]').click()
    expect(page.locator("#queueFilter")).to_have_value("all")
    expect(page.locator("#queueCount")).to_contain_text("All: 2 of 2 bursts")


def test_classic_pipeline_review_opens_requested_burst_from_url(live_server, page):
    image_body = base64.b64decode(_PNG_1X1)
    results = {
        "photos": [
            {"id": 1, "filename": "a.jpg", "label": "REVIEW", "quality_composite": 0.3, "subject_tenengrad": 10},
            {"id": 2, "filename": "b.jpg", "label": "REVIEW", "quality_composite": 0.6, "subject_tenengrad": 20},
        ],
        "encounters": [
            {
                "photo_ids": [1, 2],
                "photo_count": 2,
                "burst_count": 1,
                "species": ["Test bird"],
                "bursts": [{"photo_ids": [1, 2]}],
            }
        ],
        "summary": {
            "total_photos": 2,
            "encounter_count": 1,
            "burst_count": 1,
            "keep_count": 0,
            "review_count": 2,
            "reject_count": 0,
        },
    }
    page.route(
        "**/api/pipeline/page-init",
        lambda route: route.fulfill(
            json={
                "results": results,
                "workspace_overrides": {},
                "review_readiness": {"state": "ready", "total_photos": 2},
            }
        ),
    )
    page.route(
        "**/api/pipeline/group/state",
        lambda route: route.fulfill(
            json={
                "photos": {
                    "1": {"flag": "none", "has_species_keyword": False},
                    "2": {"flag": "none", "has_species_keyword": False},
                },
                "species_kid": None,
            }
        ),
    )
    page.route("**/photos/*/preview?*", lambda route: route.fulfill(body=image_body, content_type="image/png"))
    page.route("**/photos/*/original", lambda route: route.fulfill(body=image_body, content_type="image/png"))

    page.goto(f"{live_server['url']}/pipeline/review?enc=0&burst=0")

    expect(page.locator("#grmOverlay")).to_have_class(re.compile(r"\bopen\b"))
    expect(page.locator("#grmOverlay .grm-card[data-photo-id]")).to_have_count(2)
