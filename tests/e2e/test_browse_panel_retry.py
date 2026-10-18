"""Failures in Browse's read-only panels can recover without losing selection."""

import pytest
from playwright.sync_api import expect

PANELS = [
    ("keywords", "selectionKeywordSuggestions", "keyword-suggestions", "Retry loading keyword suggestions"),
    ("predictions", "selectionPredictions", "prediction-suggestions", "Retry loading selection predictions"),
]


def select_pair(page, first=0):
    cards = page.locator("#grid .grid-card")
    cards.nth(first).click()
    cards.nth(first + 1).click(modifiers=["ControlOrMeta"])
    expect(page.locator("#selectionPanel")).to_be_visible()
    ids = page.evaluate("pageModules.selection.getActiveSelection()")
    assert len(ids) == 2
    return sorted(ids)


@pytest.mark.parametrize("kind,panel_id,endpoint,label", PANELS)
def test_selection_panel_retry_recovers_without_reselection(live_server, page, kind, panel_id, endpoint, label):
    requests = []
    held = []

    def intercept(route):
        requests.append(route.request.post_data_json["photo_ids"])
        if len(requests) == 1:
            route.fulfill(status=503, json={"error": "Temporary failure"})
        else:
            held.append(route)

    pattern = f"**/api/selection/{endpoint}"
    page.route(pattern, intercept)
    page.goto(f"{live_server['url']}/browse")
    selected = select_pair(page)
    panel = page.locator(f"#{panel_id}")
    retry = panel.get_by_role("button", name=label, exact=True)
    expect(retry).to_be_visible()
    with page.expect_request(pattern):
        retry.press("Enter")
    expect(panel).to_contain_text("Checking")
    expect(panel).to_be_focused()
    assert len(held) == 1
    assert [sorted(ids) for ids in requests] == [selected, selected]
    with page.expect_response(pattern):
        held[0].continue_()
    expect(retry).to_have_count(0)
    expect(panel).not_to_contain_text("Checking")
    expect(panel).not_to_contain_text("Could not load")
    assert sorted(page.evaluate("pageModules.selection.getActiveSelection()")) == selected


@pytest.mark.parametrize("kind,panel_id,endpoint,label", PANELS)
def test_late_retry_cannot_repaint_a_different_selection(live_server, page, kind, panel_id, endpoint, label):
    requests = []
    held = []

    def intercept(route):
        requests.append(route.request.post_data_json["photo_ids"])
        if len(requests) == 1:
            route.fulfill(status=503, json={"error": "Temporary failure"})
        elif len(requests) == 2:
            held.append(route)
        else:
            route.continue_()

    pattern = f"**/api/selection/{endpoint}"
    page.route(pattern, intercept)
    page.goto(f"{live_server['url']}/browse")
    select_pair(page)
    panel = page.locator(f"#{panel_id}")
    retry = panel.get_by_role("button", name=label, exact=True)
    expect(retry).to_be_visible()
    # Keep the first completion promise so the late-response assertion waits
    # for the actual loader instead of relying on a sleep or browser frames.
    with page.expect_request(pattern):
        page.evaluate(
            """kind => {
          window.pendingPanelRetry = pageModules.panelLoaders.panelLoaders[kind].retry();
          pageModules.panelLoaders.panelLoaders[kind].retry();
        }""",
            kind,
        )
    expect(panel).to_contain_text("Checking")
    assert len(requests) == 2
    selected = select_pair(page, first=2)
    expect(panel).not_to_contain_text("Checking")
    before = panel.inner_html()
    row = {"id": 999, "name": "Stale retry keyword"} if kind == "keywords" else {"species": "Stale retry species"}
    held[0].fulfill(json={kind: [row], "selected_count": 2})
    page.evaluate("async () => { await window.pendingPanelRetry; }")
    assert panel.inner_html() == before
    assert sorted(page.evaluate("pageModules.selection.getActiveSelection()")) == selected


def test_photo_prediction_retry_recovers_after_initial_failure(live_server, page):
    requests = []

    def intercept(route):
        requests.append(route.request.url)
        if len(requests) == 1:
            route.fulfill(status=503, json={"error": "Temporary failure"})
        else:
            route.continue_()

    pattern = "**/api/predictions?photo_ids=*"
    page.route(pattern, intercept)
    page.goto(f"{live_server['url']}/browse")
    page.locator("#grid .grid-card").first.click()
    selected = page.evaluate("browseState.selectedPhotoId")
    panel = page.locator("#detailPredictions")
    retry = panel.get_by_role("button", name="Retry loading photo predictions", exact=True)
    expect(retry).to_be_visible()
    with page.expect_response(pattern):
        retry.press("Enter")
    expect(retry).to_have_count(0)
    expect(panel).not_to_contain_text("Loading predictions")
    expect(panel).not_to_contain_text("Could not load")
    assert requests[0] == requests[1]
    assert page.evaluate("browseState.selectedPhotoId") == selected


@pytest.mark.parametrize("kind,panel_id,endpoint,label", PANELS)
@pytest.mark.parametrize("fails", [False, True])
def test_menu_action_target_does_not_replace_panel_selection(live_server, page, kind, panel_id, endpoint, label, fails):
    held = []
    pattern = f"**/api/selection/{endpoint}"
    page.route(pattern, lambda route: held.append(route))
    page.goto(f"{live_server['url']}/browse")
    selected = select_pair(page)
    panel = page.locator(f"#{panel_id}")
    expect(panel).to_contain_text("Checking")
    # Native Photo menu actions capture a lightbox photo as their write target
    # without changing the underlying Browse multi-selection.
    page.evaluate(
        """id => {
      nativeMenuSetPhotoIdsOverride([id]);
      pageModules.batchActions.batchAddKeyword();
    }""",
        selected[0],
    )
    dialog = page.locator("#batchKeywordModal")
    expect(dialog).to_be_visible()
    assert page.evaluate("pageModules.selection.getActiveSelection()") == [selected[0]]
    assert sorted(page.evaluate("pageModules.selection.getBrowseSelection()")) == selected
    with page.expect_response(pattern):
        if fails:
            held[0].fulfill(status=503, json={"error": "Temporary failure"})
        else:
            held[0].continue_()
    expect(panel).not_to_contain_text("Checking")
    dialog.get_by_role("button", name="Cancel", exact=True).click()
    assert sorted(page.evaluate("pageModules.selection.getActiveSelection()")) == selected
    retry = panel.get_by_role("button", name=label, exact=True)
    if fails:
        expect(retry).to_be_visible()
        with page.expect_request(pattern):
            retry.click()
        assert sorted(held[1].request.post_data_json["photo_ids"]) == selected
        with page.expect_response(pattern):
            held[1].continue_()
    expect(retry).to_have_count(0)
    expect(panel).not_to_contain_text("Checking")
    expect(panel).not_to_contain_text("Could not load")
