"""Highlights and Review say that they are loading, for how long, and when it failed.

Both pages fill their grid from one request that takes seconds to minutes on a
large workspace. Each test holds that request (or fails it) and reads the line
the page shows meanwhile.
"""
import re

from playwright.sync_api import expect

HIGHLIGHTS_REQUEST = re.compile(r"/api/highlights\?")
PREDICTIONS_REQUEST = re.compile(r"/api/predictions\?")
COLLECTIONS_REQUEST = re.compile(r"/api/collections$")
SERVER_ERROR = {
    "status": 500,
    "content_type": "application/json",
    "body": '{"error": "catalog is busy"}',
}


def _hold(page, pattern):
    """Park every request matching ``pattern``; the caller releases them."""
    held = []

    def park(route):
        held.append(route)

    page.route(pattern, park)

    def release():
        while held:
            held.pop().continue_()
        page.unroute(pattern)

    return held, release


def _expect_box_above_panel(page):
    """Wait until the floating box has settled clear of the open bottom panel."""
    page.wait_for_function(
        """() => {
            const box = document.querySelector('.vireo-loading-status[data-over-results]');
            const panel = document.getElementById('bottomPanel').getBoundingClientRect();
            return box && panel.height > 100
                && box.getBoundingClientRect().bottom <= panel.top;
        }"""
    )


def _seed_highlights(db, data):
    """A quality score and a species keyword on every photo, so each is a highlight."""
    species = {
        name: db.conn.execute(
            "SELECT id FROM keywords WHERE name = ?", (name,)
        ).fetchone()["id"]
        for name in ("Red-tailed Hawk", "American Robin")
    }
    for index, photo_id in enumerate(data["photos"]):
        keyword_id = species["Red-tailed Hawk" if index < 3 else "American Robin"]
        db.conn.execute(
            "UPDATE photos SET quality_score = ? WHERE id = ?",
            (0.9 - index * 0.05, photo_id),
        )
        db.conn.execute(
            "INSERT OR IGNORE INTO photo_keywords (photo_id, keyword_id) VALUES (?, ?)",
            (photo_id, keyword_id),
        )
    db.conn.commit()


def test_highlights_says_it_is_loading_and_counts_a_long_wait(live_server, page):
    _seed_highlights(live_server["db"], live_server["data"])
    _held, release = _hold(page, HIGHLIGHTS_REQUEST)
    page.clock.install()

    with page.expect_request(HIGHLIGHTS_REQUEST):
        page.goto(live_server["url"] + "/highlights")
    status = page.locator("#highlightsStatus")
    expect(status).to_be_visible()
    expect(status.locator(".vireo-loading-status-message")).to_have_text("Loading highlights…")
    # A short wait shows neither the seconds nor the hint.
    expect(status.locator(".vireo-loading-status-elapsed")).to_have_text("")
    expect(status.locator(".vireo-loading-status-hint")).to_be_hidden()

    page.clock.fast_forward(6000)
    expect(status.locator(".vireo-loading-status-elapsed")).to_have_text(re.compile(r"^ \d+s$"))
    expect(status.locator(".vireo-loading-status-hint")).to_contain_text(
        "every species in this workspace"
    )
    # The seconds are not read out one by one.
    expect(status.locator(".vireo-loading-status-elapsed")).to_have_attribute("aria-hidden", "true")

    release()
    expect(page.locator(".highlights-card").first).to_be_visible()
    expect(status).to_be_hidden()


def test_highlights_reload_dims_the_old_rows_until_the_new_ones_arrive(live_server, page):
    _seed_highlights(live_server["db"], live_server["data"])
    page.clock.install()
    page.goto(live_server["url"] + "/highlights")
    expect(page.locator(".highlights-card").first).to_be_visible()
    status = page.locator("#highlightsStatus")
    content = page.locator("#content")
    expect(status).to_be_hidden()

    _held, release = _hold(page, HIGHLIGHTS_REQUEST)
    with page.expect_request(HIGHLIGHTS_REQUEST):
        page.select_option("#confirmationFilter", "confirmed")
    page.clock.fast_forward(1000)
    expect(status).to_be_visible()
    expect(status).to_have_attribute("data-over-results", "true")
    expect(content).to_have_attribute("data-vireo-stale", "true")
    # The box floats clear of the filter controls above and the status bar below.
    box = status.bounding_box()
    controls = page.locator("#controlsBar").bounding_box()
    status_bar_top = page.locator("#bottomPanel").bounding_box()["y"]
    assert box["y"] > controls["y"] + controls["height"]
    assert box["y"] + box["height"] <= status_bar_top

    # Opening the bottom panel moves the box up with it, and closing it brings
    # the box back down.
    page.locator("#bpArrow").click()
    expect(page.locator("#bottomPanel")).to_have_class(re.compile(r"\bopen\b"))
    _expect_box_above_panel(page)
    assert status.bounding_box()["y"] < box["y"]
    page.locator("#bpArrow").click()
    expect(page.locator("#bottomPanel")).not_to_have_class(re.compile(r"\bopen\b"))
    page.wait_for_function(
        "y => document.getElementById('highlightsStatus').getBoundingClientRect().top >= y",
        arg=box["y"],
    )

    release()
    expect(status).to_be_hidden()
    expect(content).not_to_have_attribute("data-vireo-stale", "true")


def test_highlights_failure_stays_on_the_page_with_a_retry(live_server, page):
    _seed_highlights(live_server["db"], live_server["data"])
    page.route(HIGHLIGHTS_REQUEST, lambda route: route.fulfill(**SERVER_ERROR))

    page.goto(live_server["url"] + "/highlights")
    status = page.locator("#highlightsStatus")
    expect(status).to_have_attribute("data-state", "error")
    expect(status).to_contain_text("Could not load highlights")
    expect(status).to_contain_text("catalog is busy")

    page.unroute(HIGHLIGHTS_REQUEST)
    status.get_by_role("button", name="Try again").click()
    expect(page.locator(".highlights-card").first).to_be_visible()
    expect(status).to_be_hidden()


def test_review_says_it_is_loading_before_its_collection_list_returns(live_server, page):
    _held, release = _hold(page, COLLECTIONS_REQUEST)

    with page.expect_request(COLLECTIONS_REQUEST):
        page.goto(live_server["url"] + "/review")
    status = page.locator("#loading")
    expect(status).to_be_visible()
    expect(status.locator(".vireo-loading-status-message")).to_have_text("Loading photos…")
    expect(page.locator("#grid .card")).to_have_count(0)

    release()
    expect(page.locator("#grid .card").first).to_be_visible()
    expect(status).to_be_hidden()


def test_review_names_the_whole_workspace_on_a_long_wait(live_server, page):
    _held, release = _hold(page, PREDICTIONS_REQUEST)
    page.clock.install()

    with page.expect_request(PREDICTIONS_REQUEST):
        page.goto(live_server["url"] + "/review")
    status = page.locator("#loading")
    page.clock.fast_forward(6000)
    expect(status.locator(".vireo-loading-status-elapsed")).to_have_text(re.compile(r"^ \d+s$"))
    expect(status.locator(".vireo-loading-status-hint")).to_contain_text(
        "All collections loads every prediction in this workspace"
    )

    release()
    expect(page.locator("#grid .card").first).to_be_visible()
    expect(status).to_be_hidden()


def test_review_failure_stays_on_the_page_with_a_retry(live_server, page):
    page.route(PREDICTIONS_REQUEST, lambda route: route.fulfill(**SERVER_ERROR))

    page.goto(live_server["url"] + "/review")
    status = page.locator("#loading")
    expect(status).to_have_attribute("data-state", "error")
    expect(status).to_contain_text("Could not load predictions")

    page.unroute(PREDICTIONS_REQUEST)
    status.get_by_role("button", name="Try again").click()
    expect(page.locator("#grid .card").first).to_be_visible()
    expect(status).to_be_hidden()
    # The next load starts from the loading message, not the old error.
    assert status.get_attribute("data-state") is None
