import base64
import re

from playwright.sync_api import expect

_PNG_1X1 = (
    "iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAQAAAC1HAwCAAAAC0lEQVR42mP8"
    "/x8AAwMCAO+/p9sAAAAASUVORK5CYII="
)


def _mock_pipeline_rapid_review(
    page,
    *,
    results=None,
    state_ok=True,
    apply_photos=None,
    save_payloads=None,
    preview_body=None,
    preview_content_type="image/png",
    original_body=None,
    original_content_type="image/png",
):
    image_body = base64.b64decode(_PNG_1X1)
    shortcut_config = {
        "keyboard_shortcuts": {
            # Keep the old collision in this fixture so Rapid Review proves
            # page shortcuts win even when an existing config still has P for
            # Pipeline.
            "navigation": {"pipeline": "p"},
            "pipeline_rapid_review": {
                "pick": "p",
                "reject": "x",
                "next": "arrowright",
                "back": "arrowleft",
                "clear": "u",
                "apply": "enter",
                "exit": "escape",
                "zoom": "z",
            },
        }
    }
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

    page.route("**/api/config", lambda route: route.fulfill(json=shortcut_config))
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
        lambda route: route.fulfill(body=preview_body or image_body, content_type=preview_content_type),
    )
    page.route(
        "**/photos/*/original",
        lambda route: route.fulfill(body=original_body or image_body, content_type=original_content_type),
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


def test_rapid_review_pick_key_wins_over_pipeline_nav_shortcut(live_server, page):
    _mock_pipeline_rapid_review(page)

    page.goto(f"{live_server['url']}/pipeline/rapid-review")
    page.wait_for_function(
        """() => window.Keymap
          && window.Keymap.getScope() === 'pipeline_rapid_review'
          && window.Keymap.shortcutsForScope('global').some(s => s.name === 'pipeline' && s.key === 'p')"""
    )

    page.keyboard.press("p")

    expect(page.locator("#filename")).to_have_text("b.jpg")
    expect(page.locator("#pickCount")).to_have_text("1")
    assert page.url.endswith("/pipeline/rapid-review")


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


def test_rapid_review_click_main_image_opens_original_at_one_to_one(live_server, page):
    original_svg = (
        '<svg xmlns="http://www.w3.org/2000/svg" width="3000" height="2000">'
        '<rect width="3000" height="2000" fill="#101820"/>'
        '<circle cx="2600" cy="1700" r="180" fill="#f2aa4c"/>'
        "</svg>"
    )
    _mock_pipeline_rapid_review(
        page,
        preview_body=original_svg,
        preview_content_type="image/svg+xml",
        original_body=original_svg,
        original_content_type="image/svg+xml",
    )

    page.goto(f"{live_server['url']}/pipeline/rapid-review")
    img = page.locator("#currentPhoto")
    stage = page.locator("#photoStage")

    stage.click(position={"x": 12, "y": 12})

    expect(stage).to_have_class(re.compile(r"\bzoomed\b"))
    expect(img).to_have_attribute("src", re.compile(r"/photos/1/original$"))
    page.wait_for_function(
        """() => {
          const img = document.getElementById('currentPhoto');
          return img && img.naturalWidth === 3000 && img.style.width === '3000px';
        }"""
    )
    assert stage.evaluate("el => el.scrollWidth") >= 3000

    stage.click(position={"x": 12, "y": 12})

    expect(stage).not_to_have_class(re.compile(r"\bzoomed\b"))
    expect(img).to_have_attribute("src", re.compile(r"/photos/1/preview\?size=2560$"))

    page.keyboard.press("z")

    expect(stage).to_have_class(re.compile(r"\bzoomed\b"))
    expect(img).to_have_attribute("src", re.compile(r"/photos/1/original$"))

    page.keyboard.press("z")

    expect(stage).not_to_have_class(re.compile(r"\bzoomed\b"))
    expect(img).to_have_attribute("src", re.compile(r"/photos/1/preview\?size=2560$"))


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
