import re
from urllib.parse import parse_qs, urlparse

from playwright.sync_api import expect


def test_dashboard_scope_and_chart_drill_down(live_server, page):
    """Folder scope updates every card and follows charts into filtered Browse."""
    page_errors = []
    page.on("pageerror", lambda error: page_errors.append(str(error)))
    page.goto(f"{live_server['url']}/dashboard")

    expect(page.locator("#photoCount")).to_have_text("5")
    expect(page.locator("#attentionGrid .attention-card")).to_have_count(5)

    yard_id = live_server["data"]["folders"][1]
    page.locator("#scopeFolder").select_option(str(yard_id))
    page.locator("#scopePanel").get_by_role("button", name="Apply").click()

    expect(page.locator("#photoCount")).to_have_text("2")
    expect(page.locator("#monthChart .month-bar")).to_have_count(1)
    expect(page.locator("#monthChart .month-bar .bar-label")).to_have_text("2024-06")
    expect(page.locator("#classInvSection")).to_be_hidden()
    expect(page).to_have_url(re.compile(rf"/dashboard\?folder_id={yard_id}$"))

    page.locator("#speciesChart .species-bar", has_text="American Robin").click()
    expect(page).to_have_url(re.compile(r"/browse\?"))
    query = parse_qs(urlparse(page.url).query)
    assert query["folder_id"] == [str(yard_id)]
    assert query["keyword"] == ["American Robin"]
    expect(page.locator(".grid-card")).to_have_count(1)
    expect(page.locator(".grid-card-name")).to_have_text("robin1.jpg")
    assert page_errors == []


def test_dashboard_preview_action_opens_narrow_process_setup(live_server, page):
    """Needs Attention shortcuts prepare work without starting it automatically."""
    page.goto(f"{live_server['url']}/dashboard")
    preview_card = page.locator("#attentionGrid .attention-card", has_text="Previews")
    expect(preview_card.locator(".attention-count")).to_have_text("5")
    preview_card.get_by_role("button", name="Open Process").click()

    expect(page).to_have_url(re.compile(r"/pipeline\?dashboard_stage=previews$"))
    expect(page.locator("#card-previews")).to_have_class(re.compile(r"\bexpanded\b"))
    expect(page.locator("#enableClassify")).not_to_be_checked()
    expect(page.locator("#btnStartPipeline")).to_be_visible()
