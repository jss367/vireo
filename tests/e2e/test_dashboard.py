import json
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

    species_row = page.locator("#speciesChart .species-bar", has_text="American Robin")
    expect(species_row).to_have_attribute("role", "link")
    species_row.focus()
    page.keyboard.press("Enter")
    expect(page).to_have_url(re.compile(r"/browse\?"))
    query = parse_qs(urlparse(page.url).query)
    assert query["folder_id"] == [str(yard_id)]
    assert query["keyword"] == ["American Robin"]
    assert query["dashboard_scope"] == ["1"]
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


def test_dashboard_collection_drill_down_is_explicitly_composable(live_server, page):
    """Dashboard marks combined collection links without changing plain deep links."""
    db = live_server["db"]
    photo_ids = live_server["data"]["photos"]
    collection_id = db.add_collection(
        "Two seasons",
        json.dumps([{"field": "photo_ids", "value": [photo_ids[0], photo_ids[3]]}]),
    )

    page.goto(f"{live_server['url']}/dashboard")
    page.locator("#scopeCollection").select_option(str(collection_id))
    page.locator("#scopePanel").get_by_role("button", name="Apply").click()
    expect(page.locator("#photoCount")).to_have_text("2")
    page.locator("#monthChart .month-bar", has_text="2024-06").click()

    query = parse_qs(urlparse(page.url).query)
    assert query["dashboard_scope"] == ["1"]
    assert query["collection_id"] == [str(collection_id)]
    assert query["date_from"] == ["2024-06-01"]
    expect(page.locator(".grid-card-name")).to_have_text("robin1.jpg")

    page.goto(f"{live_server['url']}/browse?collection_id={collection_id}")
    expect(page.locator(".grid-card")).to_have_count(2)


def test_dashboard_rejects_reversed_url_dates_and_labels_open_ranges(live_server, page):
    """Bookmarked dates are validated and summaries match open-ended semantics."""
    page.goto(
        f"{live_server['url']}/dashboard?date_from=2024-07-01&date_to=2024-06-01"
    )
    expect(page).to_have_url(re.compile(r"/dashboard$"))
    expect(page.locator("#photoCount")).to_have_text("5")

    page.goto(f"{live_server['url']}/dashboard?date_from=2024-04-01")
    expect(page.locator("#scopeSummary")).to_have_text("From 2024-04-01 onward")
    expect(page.locator("#photoCount")).to_have_text("2")


def test_scoped_dashboard_disables_workspace_wide_sync(live_server, page):
    """Scoped attention counts cannot trigger workspace-wide actions."""
    db = live_server["db"]
    db.conn.execute(
        "UPDATE photos SET file_hash = 'same' WHERE id IN (?, ?)",
        tuple(live_server["data"]["photos"][:2]),
    )
    db.conn.execute(
        "INSERT INTO pending_changes "
        "(photo_id, change_type, value, change_token, workspace_id) "
        "VALUES (?, 'rating', '4', 'dashboard-test', ?)",
        (live_server["data"]["photos"][0], db._ws_id()),
    )
    db.conn.commit()

    page.goto(f"{live_server['url']}/dashboard")
    page.locator("#scopeFolder").select_option(str(live_server["data"]["folders"][0]))
    page.locator("#scopePanel").get_by_role("button", name="Apply").click()
    sync_card = page.locator("#attentionGrid .attention-card", has_text="Metadata sync")
    expect(sync_card.locator(".attention-count")).to_have_text("1")
    sync_button = sync_card.get_by_role("button", name="Sync workspace")
    expect(sync_button).to_be_disabled()
    expect(sync_button).to_have_attribute("title", re.compile("workspace-wide"))
    duplicate_card = page.locator(
        "#attentionGrid .attention-card", has_text="Duplicate groups"
    )
    expect(duplicate_card.locator(".attention-count")).to_have_text("1")
    duplicate_button = duplicate_card.get_by_role("button", name="Review duplicates")
    expect(duplicate_button).to_be_disabled()
    expect(duplicate_button).to_have_attribute("title", re.compile("workspace-wide"))


def test_process_blocks_unavailable_dashboard_collection(live_server, page):
    """A stale Dashboard shortcut cannot fall back to a different Process source."""
    collection_id = live_server["db"].add_collection("Broken", "{not json")
    page.goto(
        f"{live_server['url']}/pipeline?dashboard_stage=previews"
        f"&collection_id={collection_id}"
    )
    expect(page.locator("#btnStartPipeline")).to_be_disabled()
    expect(page.locator("#pipelineActionStatus")).to_contain_text(
        "Dashboard collection is unavailable"
    )
