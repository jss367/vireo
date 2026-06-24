import json
import re

from playwright.sync_api import expect


def test_move_folder_button_shows_preflight_progress(live_server, page):
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
    expect(btn).to_have_text("Move Folder")
    expect(btn).to_be_enabled()
