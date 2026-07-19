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
    expect(btn).to_have_text("Checking destination…")

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
    expect(btn).to_contain_text("Review move")
    expect(btn).to_be_enabled()


def test_existing_destination_confirmation_is_scannable(live_server, page):
    live_server["db"].update_folder_counts()
    page.goto(f"{live_server['url']}/move")

    page.route(
        "**/api/move-folder/preflight",
        lambda route: route.fulfill(
            status=200,
            content_type="application/json",
            body=json.dumps({
                "resolved_dest": "/archive/park",
                "exists": True,
                "file_count": 8,
                "file_count_truncated": False,
                "preview": {
                    "will_copy": 5,
                    "will_skip": 3,
                    "will_block": 0,
                },
            }),
        ),
    )
    page.locator("#quickFolderSelect").select_option(index=1)
    page.locator("#quickDestInput").fill("/archive")
    page.locator("#quickMoveBtn").click()

    modal = page.locator("#confirmModal")
    expect(modal).to_have_class(re.compile(r"\bopen\b"))
    expect(modal.locator("#confirmEyebrow")).to_have_text(
        "Destination already exists"
    )
    expect(modal.locator("#confirmTitle")).to_have_text(
        "Merge with the existing folder?"
    )
    expect(modal.locator(".confirm-route-path").nth(0)).to_have_text(
        "/photos/park"
    )
    expect(modal.locator(".confirm-route-path").nth(1)).to_have_text(
        "/archive/park"
    )
    stats = modal.locator(".confirm-stat")
    expect(stats.nth(0).locator(".confirm-stat-value")).to_have_text("3")
    expect(stats.nth(0).locator(".confirm-stat-label")).to_have_text(
        "photos in source"
    )
    expect(stats.nth(1).locator(".confirm-stat-value")).to_have_text("5")
    expect(stats.nth(1).locator(".confirm-stat-label")).to_have_text(
        "files to copy"
    )
    expect(stats.nth(2).locator(".confirm-stat-value")).to_have_text("3")
    expect(stats.nth(2).locator(".confirm-stat-label")).to_have_text(
        "already present and kept"
    )
    expect(modal.locator(".confirm-note").last).to_contain_text(
        "Originals are removed only after every source file is verified"
    )
    expect(modal.locator("#confirmOkBtn")).to_have_text("Merge & Resume")


def test_existing_destination_confirmation_scrolls_on_short_viewport(
    live_server, page
):
    live_server["db"].update_folder_counts()
    page.set_viewport_size({"width": 800, "height": 420})
    page.goto(f"{live_server['url']}/move")

    page.route(
        "**/api/move-folder/preflight",
        lambda route: route.fulfill(
            status=200,
            content_type="application/json",
            body=json.dumps({
                "resolved_dest": (
                    "/archive/long-destination-name/another-long-segment/park"
                ),
                "exists": True,
                "file_count": 8,
                "file_count_truncated": False,
                "preview": {
                    "will_copy": 5,
                    "will_skip": 3,
                    "will_block": 1,
                },
            }),
        ),
    )
    page.locator("#quickFolderSelect").select_option(index=1)
    page.locator("#quickDestInput").fill("/archive")
    page.locator("#quickMoveBtn").click()

    modal = page.locator("#confirmModal .move-confirm-modal")
    content = modal.locator(".confirm-content")
    actions = modal.locator(".confirm-actions")
    expect(actions).to_be_in_viewport()
    expect(modal.locator("#confirmOkBtn")).to_be_visible()
    assert content.evaluate("el => el.scrollHeight > el.clientHeight")


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
    route_paths = summary.locator(".move-route-path")
    expect(route_paths.nth(0)).to_have_text("/photos/park")
    expect(route_paths.nth(1)).to_have_text("/archive/2024-03-10/park")
    expect(summary.locator(".move-preview-meta")).to_contain_text("3 photos")


def test_move_folder_can_be_renamed_in_final_location_preview(live_server, page):
    live_server["db"].update_folder_counts()
    url = live_server["url"]
    page.goto(f"{url}/move")

    page.locator("#quickFolderSelect").select_option(index=1)
    expect(page.locator("#quickFolderName")).to_have_value("park")

    page.locator("#quickDestInput").fill("/archive/2026")
    page.locator("#quickFolderName").fill("2026-07-12")

    summary = page.locator("#quickMoveSummary")
    expect(summary).to_be_visible()
    expect(summary.locator(".move-route-path").nth(1)).to_have_text(
        "/archive/2026/2026-07-12"
    )
    expect(summary.locator(".move-preview-meta")).to_contain_text(
        "Folder will be renamed"
    )
    expect(page.locator("#quickMoveBtn")).to_be_enabled()


def test_move_folder_preserves_untouched_name_with_surrounding_spaces(
    live_server, page
):
    """An unchanged folder name keeps its original whitespace.

    A .trim() on submit would silently rename e.g. ' Shoot ' to 'Shoot' on
    a no-op move — merging with a different existing folder. Leaving the
    pre-populated name untouched must round-trip verbatim.
    """
    db = live_server["db"]
    fid = db.add_folder("/photos/ shoot ", name=" shoot ")
    db.add_photo(
        folder_id=fid, filename="a.jpg", extension=".jpg",
        file_size=1000, file_mtime=1.0, timestamp="2024-04-01T10:00:00",
    )
    db.update_folder_counts()
    url = live_server["url"]
    page.goto(f"{url}/move")

    page.locator("#quickFolderSelect").select_option(value=str(fid))
    expect(page.locator("#quickFolderName")).to_have_value(" shoot ")

    page.locator("#quickDestInput").fill("/archive/2026")

    summary = page.locator("#quickMoveSummary")
    expect(summary).to_be_visible()
    # The trailing whitespace is preserved on the destination leaf.
    expect(summary.locator(".move-route-path").nth(1)).to_have_text(
        "/archive/2026/ shoot "
    )
    # No rename notice — the user hasn't actually renamed anything.
    expect(summary.locator(".move-preview-meta")).not_to_contain_text(
        "Folder will be renamed"
    )
    expect(page.locator("#quickMoveBtn")).to_be_enabled()
