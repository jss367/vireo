import json
import re

from playwright.sync_api import expect


def test_move_folder_button_shows_preflight_progress(live_server, page):
    live_server["db"].update_folder_counts()
    url = live_server["url"]
    page.goto(f"{url}/move")

    held_routes = []

    def hold_preflight(route):
        held_routes.append(route)

    page.route("**/api/move-folder/preflight", hold_preflight)
    page.locator("#quickFolderSelect").select_option(index=1)
    page.locator("#quickDestInput").fill("/tmp/vireo-archive")

    btn = page.locator("#quickMoveBtn")
    expect(btn).to_be_enabled()
    btn.click()

    for _ in range(50):
        if held_routes:
            break
        page.wait_for_timeout(100)
    assert held_routes

    expect(btn).to_be_disabled()
    expect(btn).to_have_text("Checking...")

    held_routes[0].fulfill(
        status=200,
        content_type="application/json",
        body=json.dumps({
            "resolved_dest": "/tmp/vireo-archive/park",
            "exists": False,
            "file_count": 0,
            "file_count_truncated": False,
        }),
    )
    expect(page.locator("#confirmModal")).to_have_class(re.compile(r"\bopen\b"))
    expect(page.locator("#confirmMessage")).to_contain_text(
        "Confirm moving 3 photos from /photos/park to "
        "/tmp/vireo-archive/park?"
    )
    expect(btn).to_have_text("Move Folder")
    expect(btn).to_be_enabled()


def test_move_folder_shows_source_while_choosing_destination(live_server, page):
    live_server["db"].update_folder_counts()
    url = live_server["url"]
    page.goto(f"{url}/move")

    page.locator("#quickFolderSelect").select_option(index=1)
    page.get_by_role("button", name="Browse", exact=True).first.click()

    expect(page.locator("#moveBrowserSource")).to_be_visible()
    expect(page.locator("#moveBrowserSourcePath")).to_have_text("/photos/park")
    expect(page.locator("#moveBrowserSourceCount")).to_have_text("(3 photos)")


def test_move_folder_prints_full_move_before_submission(live_server, page):
    live_server["db"].update_folder_counts()
    url = live_server["url"]
    page.goto(f"{url}/move")

    page.locator("#quickFolderSelect").select_option(index=1)
    page.locator("#quickDestInput").fill("/archive/2024-03-10")

    summary = page.locator("#quickMoveSummary")
    expect(summary).to_be_visible()
    expect(summary).to_have_text(
        "Confirm moving 3 photos from /photos/park to "
        "/archive/2024-03-10/park?"
    )
