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
    shortcut_config=None,
    state_photos=None,
    species_payloads=None,
    species_response=None,
):
    image_body = base64.b64decode(_PNG_1X1)
    if shortcut_config is None:
        shortcut_config = {
            "keyboard_shortcuts": {
                # Keep the old collision in this fixture so Rapid Review proves
                # legacy bare navigation shortcuts are ignored and leave P for
                # the page-local Pick action.
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
        if state_photos is None:
            state_photos = {
                "1": {"flag": "none", "has_species_keyword": False},
                "2": {"flag": "none", "has_species_keyword": False},
                "3": {"flag": "none", "has_species_keyword": False},
            }
        page.route(
            "**/api/pipeline/group/state",
            lambda route: route.fulfill(
                json={
                    "photos": state_photos,
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

    # /group/apply is flags-only now; species confirmation routes through the
    # unified /api/encounters/species endpoint. By default echo back the current
    # structure (no-op adopt). Tests can capture request bodies via
    # species_payloads and override the adopted structure via species_response
    # (e.g. to simulate the server's auto-detach restructure).
    def encounters_species(route):
        if species_payloads is not None:
            species_payloads.append(route.request.post_data_json)
        body = species_response if species_response is not None else {
            "ok": True,
            "encounters": results["encounters"],
            "summary": results["summary"],
        }
        route.fulfill(json=body)

    page.route("**/api/encounters/species", encounters_species)
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
    expect(page.locator("#applyBtn")).to_be_enabled()

    page.keyboard.press("x")
    expect(page.locator("#filename")).to_have_text("b.jpg")
    expect(page.locator("#rejectCount")).to_have_text("1")

    page.keyboard.press("ArrowRight")
    expect(page.locator("#filename")).to_have_text("c.jpg")
    expect(page.locator("#metricReviewed")).to_have_text("2/3")

    page.keyboard.press("ArrowLeft")
    expect(page.locator("#filename")).to_have_text("b.jpg")
    expect(page.locator("#metricReviewed")).to_have_text("1/3")


def test_rapid_review_pick_key_ignores_legacy_pipeline_nav_shortcut(live_server, page):
    _mock_pipeline_rapid_review(page)

    page.goto(f"{live_server['url']}/pipeline/rapid-review")
    page.wait_for_function(
        """() => window.Keymap
          && window.Keymap.getScope() === 'pipeline_rapid_review'
          && !window.Keymap.shortcutsForScope('global').some(s => s.name === 'pipeline' && s.key === 'p')"""
    )

    page.keyboard.press("p")

    expect(page.locator("#filename")).to_have_text("b.jpg")
    expect(page.locator("#pickCount")).to_have_text("1")
    assert page.url.endswith("/pipeline/rapid-review")


def test_rapid_review_honors_remapped_decision_shortcuts(live_server, page):
    _mock_pipeline_rapid_review(
        page,
        shortcut_config={
            "keyboard_shortcuts": {
                "navigation": {"pipeline": "p"},
                "pipeline_rapid_review": {
                    "pick": "k",
                    "reject": "j",
                    "next": "n",
                    "back": "h",
                    "clear": "u",
                    "apply": "enter",
                    "exit": "escape",
                    "zoom": "z",
                },
            }
        },
    )

    page.goto(f"{live_server['url']}/pipeline/rapid-review")
    page.wait_for_function(
        """() => window._vireoShortcuts
          && window._vireoShortcuts.pipeline_rapid_review
          && window._vireoShortcuts.pipeline_rapid_review.reject === 'j'"""
    )

    page.keyboard.press("ArrowDown")
    expect(page.locator("#filename")).to_have_text("a.jpg")
    expect(page.locator("#rejectCount")).to_have_text("0")

    page.keyboard.press("j")
    expect(page.locator("#filename")).to_have_text("b.jpg")
    expect(page.locator("#rejectCount")).to_have_text("1")

    page.keyboard.press("n")
    expect(page.locator("#filename")).to_have_text("c.jpg")
    expect(page.locator("#metricReviewed")).to_have_text("2/3")

    page.keyboard.press("h")
    expect(page.locator("#filename")).to_have_text("b.jpg")
    expect(page.locator("#metricReviewed")).to_have_text("1/3")


def test_rapid_review_keeps_apply_disabled_when_state_load_fails(live_server, page):
    _mock_pipeline_rapid_review(page, state_ok=False)

    page.goto(f"{live_server['url']}/pipeline/rapid-review")

    expect(page.locator("#filename")).to_have_text("a.jpg")
    expect(page.locator("#applyBtn")).to_be_disabled()
    expect(page.locator("#burstSubtitle")).to_contain_text("Failed to load burst state")

    page.keyboard.press("x")
    expect(page.locator("#filename")).to_have_text("a.jpg")
    expect(page.locator("#rejectCount")).to_have_text("0")


def test_rapid_review_apply_button_summarizes_pending_writes(live_server, page):
    _mock_pipeline_rapid_review(page)

    page.goto(f"{live_server['url']}/pipeline/rapid-review")

    # Species tagging now covers EVERY frame in the burst (not just picks), so
    # with a pre-filled species and no frames yet carrying the keyword, Apply
    # would tag all 3 — the label must say so before any pick.
    expect(page.locator("#applyBtn")).to_have_text("Apply: Tag 3")

    page.keyboard.press("p")
    expect(page.locator("#applyBtn")).to_have_text("Apply: Flag 1 · Tag 3")

    page.keyboard.press("x")
    expect(page.locator("#applyBtn")).to_have_text("Apply: Flag 1 · Reject 1 · Tag 3")
    expect(page.locator("#applyBtn")).to_have_attribute(
        "title",
        'Apply will flag 1 photo as a pick, reject 1 photo, add species keyword "Test bird" to 3 burst frames.',
    )


def test_rapid_review_species_edit_refreshes_existing_keyword_state(live_server, page):
    results = {
        "photos": [
            {"id": 1, "filename": "a.jpg", "label": "KEEP", "flag": "flagged"},
        ],
        "encounters": [
            {
                "photo_ids": [1],
                "photo_count": 1,
                "burst_count": 1,
                "species": ["Test bird"],
                "bursts": [{"photo_ids": [1]}],
            }
        ],
        "summary": {"keep_count": 1, "review_count": 0, "reject_count": 0},
    }
    _mock_pipeline_rapid_review(
        page,
        results=results,
        state_photos={"1": {"flag": "flagged", "has_species_keyword": True}},
    )

    page.goto(f"{live_server['url']}/pipeline/rapid-review")
    expect(page.locator("#applyBtn")).to_have_text("Apply: no DB changes")

    page.locator("#speciesInput").fill("New bird")

    expect(page.locator("#applyBtn")).to_have_text("Apply: Set species")
    expect(page.locator("#applyBtn")).to_have_attribute(
        "title",
        'Apply will set confirmed species to "New bird".',
    )


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


def test_rapid_review_deep_link_opens_filtered_out_burst_via_all(live_server, page):
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

    page.goto(f"{live_server['url']}/pipeline/rapid-review?enc=0&burst=0")

    expect(page.locator("#queueFilter")).to_have_value("all")
    expect(page.locator("#burstTitle")).to_have_text("Encounter 1, Burst 1")
    expect(page.locator("#filename")).to_have_text("confirmed.jpg")


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
    page.keyboard.press("p")

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
    page.keyboard.press("p")

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
    save_payloads = []
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
    _mock_pipeline_rapid_review(page, results=results, save_payloads=save_payloads)

    page.goto(f"{live_server['url']}/pipeline/rapid-review")
    expect(page.locator("#applyBtn")).to_be_enabled()
    page.keyboard.press("x")
    page.locator("#queueFilter").select_option("all")

    expect(page.locator("#queuePrompt")).to_be_visible()
    page.keyboard.press("Escape")
    expect(page.locator("#queuePrompt")).to_be_visible()
    expect(page).to_have_url(re.compile(r"/pipeline/rapid-review$"))
    page.keyboard.press("Enter")
    expect(page.locator("#queuePrompt")).to_be_visible()
    assert save_payloads == []
    page.locator('[data-queue-choice="stay"]').click()
    expect(page.locator("#queueFilter")).to_have_value("needs-species")

    page.locator("#queueFilter").select_option("all")
    expect(page.locator("#queuePrompt")).to_be_visible()
    page.locator('[data-queue-choice="discard"]').click()
    expect(page.locator("#queueFilter")).to_have_value("all")
    expect(page.locator("#queueCount")).to_contain_text("All: 2 of 2 bursts")


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
        "summary": {
            "total_photos": 3,
            "encounter_count": 1,
            "burst_count": 1,
            "keep_count": 0,
            "review_count": 3,
            "reject_count": 0,
        },
    }
    page.route(
        "**/api/pipeline/page-init",
        lambda route: route.fulfill(
            json={
                "results": results,
                "workspace_overrides": {},
                "review_readiness": {"state": "ready", "total_photos": 3},
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
                    "3": {"flag": "none", "has_species_keyword": False},
                },
                "species_kid": None,
            }
        ),
    )
    page.route("**/photos/*/preview?*", lambda route: route.fulfill(body=image_body, content_type="image/png"))
    page.route("**/photos/*/original", lambda route: route.fulfill(body=image_body, content_type="image/png"))

    page.goto(f"{live_server['url']}/pipeline/review?enc=0&burst=0")

    expect(page.locator("#grmOverlay")).to_have_class(re.compile(r"\bopen\b"))
    expect(page.locator("#grmOverlay .grm-card[data-photo-id]")).to_have_count(3)
    expect(page.locator("#grmPicks .grm-card[data-photo-id]")).to_have_count(0)
    expect(page.locator("#grmRejects .grm-card[data-photo-id]")).to_have_count(0)
    expect(page.locator("#grmCandidates .grm-card[data-photo-id]")).to_have_count(3)
    expect(page.locator("#grmCount")).to_have_text("0 picks, 0 rejects, 3 unsorted")

    page.keyboard.press("x")
    expect(page.locator("#grmCount")).to_have_text("0 picks, 1 rejects, 2 unsorted")

    page.keyboard.press("p")
    expect(page.locator("#grmCount")).to_have_text("1 picks, 0 rejects, 2 unsorted")
    # The burst's species ("Test bird") is an unconfirmed prediction, so picking
    # a frame pre-checks "Confirm species" (smart default keys off confirmed
    # species, not the prediction fallback). The species is therefore set and
    # all 3 post-apply burst members are tagged.
    expect(page.locator("#grmConfirmSpeciesChk")).to_be_checked()
    expect(page.locator("#grmApplyFlagsChk")).to_be_checked()
    expect(page.locator("#grmApplyBtn")).to_have_text("Flag 1 · Set species · Tag 3 as Test bird & Close")
    expect(page.locator("#grmApplyBtn")).to_have_attribute(
        "title",
        'Apply will flag 1 photo as a pick, set confirmed species to "Test bird", '
        'add species keyword "Test bird" to 3 burst frames, then close this burst.',
    )


def test_classic_pipeline_review_single_photo_opens_review_modal(live_server, page):
    image_body = base64.b64decode(_PNG_1X1)
    results = {
        "photos": [
            {"id": 1, "filename": "solo.jpg", "label": "REVIEW", "quality_composite": 0.5, "subject_tenengrad": 10},
        ],
        "encounters": [
            {
                "photo_ids": [1],
                "photo_count": 1,
                "burst_count": 1,
                "species": ["Test bird"],
                "bursts": [{"photo_ids": [1]}],
            }
        ],
        "summary": {
            "total_photos": 1,
            "encounter_count": 1,
            "burst_count": 1,
            "keep_count": 0,
            "review_count": 1,
            "reject_count": 0,
        },
    }
    page.route(
        "**/api/pipeline/page-init",
        lambda route: route.fulfill(
            json={
                "results": results,
                "workspace_overrides": {},
                "review_readiness": {"state": "ready", "total_photos": 1},
            }
        ),
    )
    page.route(
        "**/api/pipeline/group/state",
        lambda route: route.fulfill(
            json={
                "photos": {"1": {"flag": "none", "has_species_keyword": False}},
                "species_kid": None,
            }
        ),
    )
    page.route("**/thumbnails/*.jpg", lambda route: route.fulfill(body=image_body, content_type="image/png"))
    page.route("**/photos/*/preview?*", lambda route: route.fulfill(body=image_body, content_type="image/png"))
    page.route("**/photos/*/original", lambda route: route.fulfill(body=image_body, content_type="image/png"))

    page.goto(f"{live_server['url']}/pipeline/review")
    page.locator(".photo-card img").click()
    expect(page.locator("#inspectOverlay")).not_to_have_class(re.compile(r"\bopen\b"))
    expect(page.locator("#grmOverlay")).to_have_class(re.compile(r"\bopen\b"))
    expect(page.locator("#grmTitle")).to_have_text("Review Photo")
    expect(page.locator("#grmOverlay .grm-card[data-photo-id]")).to_have_count(1)
    expect(page.locator("#grmResSlider")).to_be_visible()
    expect(page.locator("#grmLoupeZoomSlider")).to_be_visible()
    expect(page.locator("#grmRemoveBtn")).to_be_hidden()

    page.keyboard.press("Backspace")
    expect(page.locator("#grmOverlay .grm-card[data-photo-id]")).to_have_count(1)
    expect(page.locator("#grmCount")).to_have_text("0 picks, 0 rejects, 1 unsorted")

    page.keyboard.press("x")
    expect(page.locator("#grmCount")).to_have_text("0 picks, 1 rejects, 0 unsorted")
    # "Test bird" is an unconfirmed prediction here, so "Confirm species" is
    # pre-checked regardless of the cull state — the species gets set and the
    # frame tagged.
    expect(page.locator("#grmConfirmSpeciesChk")).to_be_checked()
    expect(page.locator("#grmApplyBtn")).to_have_text("Reject 1 · Set species · Tag 1 as Test bird & Close")

    page.keyboard.press("p")
    expect(page.locator("#grmCount")).to_have_text("1 picks, 0 rejects, 0 unsorted")
    # Picking the photo pre-checks both "Confirm species" (unconfirmed
    # prediction) and "Apply flags" (a pending pick), so the species is
    # confirmed/tagged on apply — the regression this guards against.
    expect(page.locator("#grmConfirmSpeciesChk")).to_be_checked()
    expect(page.locator("#grmApplyFlagsChk")).to_be_checked()
    expect(page.locator("#grmApplyBtn")).to_have_text("Flag 1 · Set species · Tag 1 as Test bird & Close")


def test_classic_pipeline_review_group_shortcuts_do_not_flag_prior_single_photo(live_server, page):
    image_body = base64.b64decode(_PNG_1X1)
    results = {
        "photos": [
            {"id": 1, "filename": "solo.jpg", "label": "REVIEW", "quality_composite": 0.5, "subject_tenengrad": 10},
            {"id": 2, "filename": "burst-a.jpg", "label": "REVIEW", "quality_composite": 0.3, "subject_tenengrad": 10},
            {"id": 3, "filename": "burst-b.jpg", "label": "REVIEW", "quality_composite": 0.6, "subject_tenengrad": 20},
        ],
        "encounters": [
            {
                "photo_ids": [1, 2, 3],
                "photo_count": 3,
                "burst_count": 2,
                "species": ["Test bird"],
                "bursts": [{"photo_ids": [1]}, {"photo_ids": [2, 3]}],
            }
        ],
        "summary": {
            "total_photos": 3,
            "encounter_count": 1,
            "burst_count": 2,
            "keep_count": 0,
            "review_count": 3,
            "reject_count": 0,
        },
    }
    stale_flag_payloads = []

    def stale_flag_route(route):
        stale_flag_payloads.append(route.request.post_data_json)
        route.fulfill(json={"ok": True})

    page.route(
        "**/api/pipeline/page-init",
        lambda route: route.fulfill(
            json={
                "results": results,
                "workspace_overrides": {},
                "review_readiness": {"state": "ready", "total_photos": 3},
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
                    "3": {"flag": "none", "has_species_keyword": False},
                },
                "species_kid": None,
            }
        ),
    )
    page.route("**/api/photos/1/flag", stale_flag_route)
    page.route("**/thumbnails/*.jpg", lambda route: route.fulfill(body=image_body, content_type="image/png"))
    page.route("**/photos/*/preview?*", lambda route: route.fulfill(body=image_body, content_type="image/png"))
    page.route("**/photos/*/original", lambda route: route.fulfill(body=image_body, content_type="image/png"))

    page.goto(f"{live_server['url']}/pipeline/review")
    page.locator(".photo-card[data-photo-id='1'] img").click()
    expect(page.locator("#inspectOverlay")).not_to_have_class(re.compile(r"\bopen\b"))
    expect(page.locator("#grmOverlay")).to_have_class(re.compile(r"\bopen\b"))
    expect(page.locator("#grmTitle")).to_have_text("Review Photo")
    expect(page.locator("#grmOverlay .grm-card[data-photo-id]")).to_have_count(1)

    page.locator("#grmOverlay .grm-close").click()
    expect(page.locator("#grmOverlay")).not_to_have_class(re.compile(r"\bopen\b"))
    page.locator(".photo-card[data-photo-id='2'] img").click()
    expect(page.locator("#grmOverlay")).to_have_class(re.compile(r"\bopen\b"))
    expect(page.locator("#grmTitle")).to_have_text("Review Burst Group")
    expect(page.locator("#grmOverlay .grm-card[data-photo-id]")).to_have_count(2)

    page.keyboard.press("x")
    expect(page.locator("#grmCount")).to_have_text("0 picks, 1 rejects, 1 unsorted")
    page.wait_for_timeout(200)
    assert stale_flag_payloads == []


def test_rapid_review_apply_tags_all_burst_frames_via_encounters_species(live_server, page):
    # A multi-frame burst where only ONE frame is a pick. Species tagging must
    # cover EVERY frame (matching the pipeline decision / the grid), so the
    # /api/encounters/species request body must carry all burst photo_ids — not
    # just the pick — plus the burst's index.
    species_payloads = []
    results = {
        "photos": [
            {"id": 1, "filename": "a.jpg", "label": "REVIEW", "flag": "none"},
            {"id": 2, "filename": "b.jpg", "label": "REVIEW", "flag": "none"},
            {"id": 3, "filename": "c.jpg", "label": "REVIEW", "flag": "none"},
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
    _mock_pipeline_rapid_review(
        page,
        results=results,
        apply_photos={
            "1": {"flag": "flagged", "has_species_keyword": False},
            "2": {"flag": "none", "has_species_keyword": False},
            "3": {"flag": "none", "has_species_keyword": False},
        },
        species_payloads=species_payloads,
    )

    page.goto(f"{live_server['url']}/pipeline/rapid-review")
    expect(page.locator("#applyBtn")).to_be_enabled()
    # Pick only the first frame; the other two stay unsorted.
    page.keyboard.press("p")

    with page.expect_response("**/api/encounters/species"):
        page.locator("#applyBtn").click()

    assert species_payloads, "expected /api/encounters/species to fire"
    body = species_payloads[-1]
    assert body["species"] == "Test bird"
    assert body["burst_index"] == 0
    # ALL burst frames, not just the single pick.
    assert sorted(body["photo_ids"]) == [1, 2, 3]


def test_rapid_review_cull_only_on_confirmed_burst_skips_species_post(live_server, page):
    # A burst ALREADY confirmed as "Test bird" (encounter species_confirmed +
    # burst override confirmed, photos carrying the keyword). Applying ONLY a
    # cull change (reject a frame) with the species field still = "Test bird"
    # must NOT post to /api/encounters/species: re-posting an unchanged species
    # would record a no-op keyword_add edit per frame whose undo strips the
    # existing species keywords. Flags must still apply + save.
    species_payloads = []
    save_payloads = []
    results = {
        "photos": [
            {"id": 1, "filename": "a.jpg", "label": "KEEP", "flag": "flagged", "confirmed_species": "Test bird"},
            {"id": 2, "filename": "b.jpg", "label": "REVIEW", "flag": "none", "confirmed_species": "Test bird"},
            {"id": 3, "filename": "c.jpg", "label": "REVIEW", "flag": "none", "confirmed_species": "Test bird"},
        ],
        "encounters": [
            {
                "photo_ids": [1, 2, 3],
                "photo_count": 3,
                "burst_count": 1,
                "species": ["Test bird"],
                "species_confirmed": True,
                "confirmed_species": "Test bird",
                "bursts": [
                    {
                        "photo_ids": [1, 2, 3],
                        "species_override": {"species": "Test bird", "confirmed": True},
                    }
                ],
            }
        ],
        "summary": {"keep_count": 1, "review_count": 2, "reject_count": 0},
    }
    _mock_pipeline_rapid_review(
        page,
        results=results,
        apply_photos={
            "1": {"flag": "rejected", "has_species_keyword": True},
            "2": {"flag": "none", "has_species_keyword": True},
            "3": {"flag": "none", "has_species_keyword": True},
        },
        state_photos={
            "1": {"flag": "flagged", "has_species_keyword": True},
            "2": {"flag": "none", "has_species_keyword": True},
            "3": {"flag": "none", "has_species_keyword": True},
        },
        species_payloads=species_payloads,
        save_payloads=save_payloads,
    )

    # Deep-link forces the "all" queue so a fully-confirmed burst is reviewable.
    page.goto(f"{live_server['url']}/pipeline/rapid-review?enc=0&burst=0")
    expect(page.locator("#applyBtn")).to_be_enabled()
    expect(page.locator("#filename")).to_have_text("a.jpg")
    # Species field already reflects the confirmed species; do NOT change it.
    expect(page.locator("#speciesInput")).to_have_value("Test bird")
    # Reject the current frame — a cull-only change.
    page.keyboard.press("x")
    expect(page.locator("#rejectCount")).to_have_text("1")

    with (
        page.expect_request("**/api/pipeline/group/apply") as apply_req,
        page.expect_response("**/api/pipeline/save-cache"),
    ):
        page.locator("#applyBtn").click()

    # No species post: re-confirming the unchanged species would be a destructive
    # no-op keyword_add.
    assert species_payloads == [], "cull-only on a confirmed burst must not re-post species"
    # Flags still applied via /group/apply (a reject was submitted) and persisted.
    apply_body = apply_req.value.post_data_json
    assert apply_body["rejects"], "expected a reject to be submitted to /group/apply"
    assert save_payloads, "expected save-cache to fire"


def test_rapid_review_confirmed_burst_with_untagged_frame_posts_species(live_server, page):
    # A burst confirmed as "Test bird" but where /group/state reports one frame
    # STILL MISSING the species keyword (e.g. legacy data that only tagged
    # picks). The species field is unchanged ("Test bird"), so the confirmed
    # species does NOT change — but rapidComputeApplyDiff advertises "Tag N", and
    # /api/pipeline/group/apply is flags-only, so the missing keyword would never
    # be written unless we still POST /api/encounters/species. The gate must fire
    # on outstanding tag work, not only on a confirmed-species change. (Regression
    # for the no-tag-on-already-confirmed-but-untagged-frame gap.)
    species_payloads = []
    save_payloads = []
    results = {
        "photos": [
            {"id": 1, "filename": "a.jpg", "label": "KEEP", "flag": "flagged", "confirmed_species": "Test bird"},
            {"id": 2, "filename": "b.jpg", "label": "REVIEW", "flag": "none", "confirmed_species": "Test bird"},
            {"id": 3, "filename": "c.jpg", "label": "REVIEW", "flag": "none", "confirmed_species": "Test bird"},
        ],
        "encounters": [
            {
                "photo_ids": [1, 2, 3],
                "photo_count": 3,
                "burst_count": 1,
                "species": ["Test bird"],
                "species_confirmed": True,
                "confirmed_species": "Test bird",
                "bursts": [
                    {
                        "photo_ids": [1, 2, 3],
                        "species_override": {"species": "Test bird", "confirmed": True},
                    }
                ],
            }
        ],
        "summary": {"keep_count": 1, "review_count": 2, "reject_count": 0},
    }
    _mock_pipeline_rapid_review(
        page,
        results=results,
        apply_photos={
            "1": {"flag": "flagged", "has_species_keyword": True},
            "2": {"flag": "none", "has_species_keyword": True},
            "3": {"flag": "none", "has_species_keyword": True},
        },
        # Frame 3 lacks the species keyword despite the burst being confirmed.
        state_photos={
            "1": {"flag": "flagged", "has_species_keyword": True},
            "2": {"flag": "none", "has_species_keyword": True},
            "3": {"flag": "none", "has_species_keyword": False},
        },
        species_payloads=species_payloads,
        save_payloads=save_payloads,
    )

    # Deep-link the "all" queue so a confirmed burst is reviewable.
    page.goto(f"{live_server['url']}/pipeline/rapid-review?enc=0&burst=0")
    expect(page.locator("#applyBtn")).to_be_enabled()
    # Species field already reflects the confirmed species; leave it unchanged.
    expect(page.locator("#speciesInput")).to_have_value("Test bird")
    # The Apply button must advertise the outstanding tag work (truthfulness).
    expect(page.locator("#applyBtn")).to_contain_text("Tag 1")

    # Apply with NO flag or species change — only the missing keyword to write.
    with page.expect_response("**/api/encounters/species"):
        page.locator("#applyBtn").click()

    # The species post fired despite the species being unchanged, so the missing
    # keyword is written to all burst frames.
    assert species_payloads, "expected /api/encounters/species to fire for outstanding tag work"
    body = species_payloads[-1]
    assert body["species"] == "Test bird"
    assert sorted(body["photo_ids"]) == [1, 2, 3]
    assert body["burst_index"] == 0


def test_rapid_review_first_confirmation_and_replacement_post_species(live_server, page):
    # Converse sanity: an UNCONFIRMED burst (no species_confirmed / no override)
    # with the species field set posts on first confirmation; changing to a
    # DIFFERENT species from the confirmed one also posts (replacement).
    species_payloads = []
    results = {
        "photos": [
            {"id": 1, "filename": "a.jpg", "label": "REVIEW", "flag": "none"},
            {"id": 2, "filename": "b.jpg", "label": "REVIEW", "flag": "none"},
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
        "summary": {"keep_count": 0, "review_count": 2, "reject_count": 0},
    }
    _mock_pipeline_rapid_review(page, results=results, species_payloads=species_payloads)

    page.goto(f"{live_server['url']}/pipeline/rapid-review")
    expect(page.locator("#applyBtn")).to_be_enabled()
    # "Test bird" is an unconfirmed prediction (no species_confirmed), so the
    # field pre-fills with it and applying is a first-time confirmation → posts.
    expect(page.locator("#speciesInput")).to_have_value("Test bird")

    with page.expect_response("**/api/encounters/species"):
        page.locator("#applyBtn").click()

    assert species_payloads, "first confirmation of an unconfirmed burst must post"
    assert species_payloads[-1]["species"] == "Test bird"


def test_rapid_review_species_replacement_on_confirmed_burst_posts(live_server, page):
    # A burst confirmed as "Test bird"; changing the field to a DIFFERENT species
    # must post (replacement), unlike the unchanged-species cull-only case.
    species_payloads = []
    results = {
        "photos": [
            {"id": 1, "filename": "a.jpg", "label": "KEEP", "flag": "flagged", "confirmed_species": "Test bird"},
            {"id": 2, "filename": "b.jpg", "label": "REVIEW", "flag": "none", "confirmed_species": "Test bird"},
        ],
        "encounters": [
            {
                "photo_ids": [1, 2],
                "photo_count": 2,
                "burst_count": 1,
                "species": ["Test bird"],
                "species_confirmed": True,
                "confirmed_species": "Test bird",
                "bursts": [
                    {"photo_ids": [1, 2], "species_override": {"species": "Test bird", "confirmed": True}}
                ],
            }
        ],
        "summary": {"keep_count": 1, "review_count": 1, "reject_count": 0},
    }
    _mock_pipeline_rapid_review(
        page,
        results=results,
        state_photos={
            "1": {"flag": "flagged", "has_species_keyword": True},
            "2": {"flag": "none", "has_species_keyword": True},
        },
        species_payloads=species_payloads,
    )

    page.goto(f"{live_server['url']}/pipeline/rapid-review?enc=0&burst=0")
    expect(page.locator("#applyBtn")).to_be_enabled()
    expect(page.locator("#speciesInput")).to_have_value("Test bird")
    page.locator("#speciesInput").fill("Different bird")

    with page.expect_response("**/api/encounters/species"):
        page.locator("#applyBtn").click()

    assert species_payloads, "species replacement must post"
    assert species_payloads[-1]["species"] == "Different bird"


def test_rapid_review_adopts_detach_restructure_without_clobbering_cache(live_server, page):
    # Simulate the server auto-detaching the burst on a species mismatch: the
    # /api/encounters/species response returns a RESTRUCTURED encounters payload
    # (one extra encounter, indices shifted). The client must adopt that exact
    # structure and the subsequent save-cache must persist it — proving there is
    # no stale-index local write that corrupts the encounter the user confirmed.
    save_payloads = []
    results = {
        "photos": [
            {"id": 1, "filename": "a.jpg", "label": "REVIEW", "flag": "none"},
            {"id": 2, "filename": "b.jpg", "label": "REVIEW", "flag": "none"},
        ],
        "encounters": [
            {
                "photo_ids": [1, 2],
                "photo_count": 2,
                "burst_count": 2,
                "species": ["Original bird"],
                "bursts": [{"photo_ids": [1]}, {"photo_ids": [2]}],
            }
        ],
        "summary": {"keep_count": 0, "review_count": 2, "reject_count": 0},
    }
    # The detached burst (photo 1, the confirmed-divergent species) becomes its
    # own new encounter; the original encounter keeps the remaining sibling.
    restructured = {
        "ok": True,
        "encounters": [
            {
                "photo_ids": [2],
                "photo_count": 1,
                "burst_count": 1,
                "species": ["Original bird"],
                "bursts": [{"photo_ids": [2]}],
            },
            {
                "photo_ids": [1],
                "photo_count": 1,
                "burst_count": 1,
                "species": ["Divergent bird"],
                "species_confirmed": True,
                "confirmed_species": "Divergent bird",
                "bursts": [{"photo_ids": [1], "species_override": {"species": "Divergent bird", "confirmed": True}}],
            },
        ],
        "summary": {"keep_count": 0, "review_count": 2, "reject_count": 0},
    }
    _mock_pipeline_rapid_review(
        page,
        results=results,
        apply_photos={"1": {"flag": "none", "has_species_keyword": False}},
        save_payloads=save_payloads,
        species_response=restructured,
    )

    page.goto(f"{live_server['url']}/pipeline/rapid-review?enc=0&burst=0")
    expect(page.locator("#applyBtn")).to_be_enabled()
    expect(page.locator("#filename")).to_have_text("a.jpg")
    # Confirm a species that diverges from the encounter's, triggering detach.
    page.locator("#speciesInput").fill("Divergent bird")

    with page.expect_response("**/api/pipeline/save-cache"):
        page.locator("#applyBtn").click()

    assert save_payloads, "expected save-cache to fire"
    saved = save_payloads[-1]
    # The client adopted the server's restructured encounters verbatim — the
    # save-cache body must match it exactly (no clobber, no stale-index write).
    assert saved["encounters"] == restructured["encounters"]
    # The detached encounter still carries its server-set confirmation; nothing
    # locally overwrote the (now index-shifted) encounter the user confirmed.
    detached = next(e for e in saved["encounters"] if e["photo_ids"] == [1])
    assert detached["species_confirmed"] is True
    assert detached["confirmed_species"] == "Divergent bird"
    # The sibling that kept the original species was NOT given the divergent one.
    sibling = next(e for e in saved["encounters"] if e["photo_ids"] == [2])
    assert sibling["species"] == ["Original bird"]
