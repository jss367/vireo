from playwright.sync_api import expect


def _pipeline_results_for_cull_keyboard():
    return {
        "photos": [
            {"id": 1, "filename": "enc1-a.jpg", "label": "KEEP", "quality_composite": 0.91},
            {"id": 2, "filename": "enc1-b.jpg", "label": "REVIEW", "quality_composite": 0.67},
            {"id": 3, "filename": "enc2-a.jpg", "label": "KEEP", "quality_composite": 0.88},
            {"id": 4, "filename": "enc3-a.jpg", "label": "REJECT", "quality_composite": 0.32},
        ],
        "encounters": [
            {
                "photo_ids": [1, 2],
                "photo_count": 2,
                "burst_count": 1,
                "species": ["Test bird"],
                "bursts": [{"photo_ids": [1, 2]}],
                "time_range": ["2024-03-10T08:00:00", "2024-03-10T08:01:00"],
            },
            {
                "photo_ids": [3],
                "photo_count": 1,
                "burst_count": 1,
                "species": ["Test bird"],
                "bursts": [{"photo_ids": [3]}],
                "time_range": ["2024-03-10T08:05:00", "2024-03-10T08:05:00"],
            },
            {
                "photo_ids": [4],
                "photo_count": 1,
                "burst_count": 1,
                "species": ["Test bird"],
                "bursts": [{"photo_ids": [4]}],
                "time_range": ["2024-03-10T08:09:00", "2024-03-10T08:09:00"],
            },
        ],
        "summary": {"keep_count": 2, "review_count": 1, "reject_count": 1},
    }


def test_cull_arrow_keys_move_between_encounters(live_server, page):
    page.route(
        "**/api/pipeline/page-init",
        lambda route: route.fulfill(json={"results": _pipeline_results_for_cull_keyboard()}),
    )

    page.goto(f"{live_server['url']}/cull")

    expect(page.locator(".pose-group")).to_have_count(3)
    expect(page.locator(".pose-group.focused .pose-label")).to_contain_text("Encounter 1")

    page.keyboard.press("ArrowRight")
    expect(page.locator(".pose-group.focused .pose-label")).to_contain_text("Encounter 2")

    page.keyboard.press("ArrowRight")
    expect(page.locator(".pose-group.focused .pose-label")).to_contain_text("Encounter 3")

    page.keyboard.press("ArrowLeft")
    expect(page.locator(".pose-group.focused .pose-label")).to_contain_text("Encounter 2")


def test_cull_arrow_keys_ignore_form_controls(live_server, page):
    page.route(
        "**/api/pipeline/page-init",
        lambda route: route.fulfill(json={"results": _pipeline_results_for_cull_keyboard()}),
    )

    page.goto(f"{live_server['url']}/cull")
    expect(page.locator(".pose-group.focused .pose-label")).to_contain_text("Encounter 1")

    page.locator("#cullCollection").focus()
    page.keyboard.press("ArrowRight")

    expect(page.locator(".pose-group.focused .pose-label")).to_contain_text("Encounter 1")


def test_cull_arrow_keys_ignore_modified_browser_shortcuts(live_server, page):
    page.route(
        "**/api/pipeline/page-init",
        lambda route: route.fulfill(json={"results": _pipeline_results_for_cull_keyboard()}),
    )

    page.goto(f"{live_server['url']}/cull")
    expect(page.locator(".pose-group.focused .pose-label")).to_contain_text("Encounter 1")

    page.keyboard.press("Alt+ArrowRight")
    expect(page.locator(".pose-group.focused .pose-label")).to_contain_text("Encounter 1")

    page.keyboard.press("Meta+ArrowRight")
    expect(page.locator(".pose-group.focused .pose-label")).to_contain_text("Encounter 1")


def test_cull_arrow_keys_ignore_active_overlays(live_server, page):
    page.route(
        "**/api/pipeline/page-init",
        lambda route: route.fulfill(json={"results": _pipeline_results_for_cull_keyboard()}),
    )

    page.goto(f"{live_server['url']}/cull")
    expect(page.locator(".pose-group.focused .pose-label")).to_contain_text("Encounter 1")

    # Simulate the pipeline inspector overlay being open (e.g. opened from the
    # cull lightbox). Arrow keys should defer to the overlay rather than moving
    # the underlying cull focus.
    page.evaluate(
        "() => { const o = document.getElementById('pipelineOverlay');"
        " if (o) o.classList.add('active'); }"
    )
    page.keyboard.press("ArrowRight")
    expect(page.locator(".pose-group.focused .pose-label")).to_contain_text("Encounter 1")

    page.evaluate(
        "() => { const o = document.getElementById('pipelineOverlay');"
        " if (o) o.classList.remove('active'); }"
    )

    # Same expectation for the Similar Photos overlay.
    page.evaluate(
        "() => { const o = document.getElementById('similarOverlay');"
        " if (o) o.classList.add('active'); }"
    )
    page.keyboard.press("ArrowRight")
    expect(page.locator(".pose-group.focused .pose-label")).to_contain_text("Encounter 1")

    page.evaluate(
        "() => { const o = document.getElementById('similarOverlay');"
        " if (o) o.classList.remove('active'); }"
    )

    # With overlays closed, arrow keys resume cull navigation.
    page.keyboard.press("ArrowRight")
    expect(page.locator(".pose-group.focused .pose-label")).to_contain_text("Encounter 2")
