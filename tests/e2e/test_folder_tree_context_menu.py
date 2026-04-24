"""E2E tests for the folder-tree right-click context menu (Task 8).

Menu items (first pass, per plan's "OPTION: skip for this first pass"):
- Filter by this folder
- separator
- Reveal in Finder/Folder
- Copy Path
- separator
- Rescan this Folder

"Expand All Children", "Collapse All Children", and "Hide from this Workspace"
are intentionally deferred — no matching helpers exist yet.
"""

from playwright.sync_api import expect


def test_folder_tree_right_click_opens_menu(live_server, page):
    """Right-clicking a folder tree item shows the folder context menu."""
    url = live_server["url"]
    page.goto(f"{url}/browse")

    item = page.locator(".tree-item[data-folder-id]").first
    item.wait_for(state="visible")
    item.click(button="right")

    menu = page.locator(".vireo-ctx-menu")
    expect(menu).to_be_visible()
    for label in [
        "Filter by this folder",
        "Reveal in",
        "Copy Path",
        "Rescan this Folder",
    ]:
        expect(
            menu.locator(".vireo-ctx-item", has_text=label)
        ).to_be_visible()


def test_folder_tree_filter_by_folder_fires_filter(live_server, page):
    """Clicking 'Filter by this folder' sets activeFolderId to that folder."""
    url = live_server["url"]
    page.goto(f"{url}/browse")

    item = page.locator(".tree-item[data-folder-id]").first
    item.wait_for(state="visible")
    fid = int(item.get_attribute("data-folder-id"))

    item.click(button="right")
    menu = page.locator(".vireo-ctx-menu")
    expect(menu).to_be_visible()

    menu.locator(".vireo-ctx-item", has_text="Filter by this folder").click()
    expect(menu).to_be_hidden()

    # filterByFolder toggles activeFolderId; the first click should set it.
    page.wait_for_function(
        f"window.activeFolderId === {fid}", timeout=3000
    )


def test_folder_tree_rescan_fires_endpoint(live_server, page):
    """Clicking 'Rescan this Folder' POSTs to /api/folders/<id>/rescan."""
    url = live_server["url"]
    page.goto(f"{url}/browse")

    item = page.locator(".tree-item[data-folder-id]").first
    item.wait_for(state="visible")
    item.click(button="right")
    menu = page.locator(".vireo-ctx-menu")
    expect(menu).to_be_visible()

    # The seed folder path doesn't exist on disk, so the server may respond
    # 400 "no longer exists" — that's fine. This test verifies the menu item
    # fires a POST at the rescan endpoint, not the job queueing itself (which
    # is covered by vireo/tests/test_folder_rescan_api.py).
    with page.expect_response(lambda r: "/rescan" in r.url):
        menu.locator(
            ".vireo-ctx-item", has_text="Rescan this Folder"
        ).click()


def test_folder_tree_reveal_fires_endpoint(live_server, page):
    """Clicking 'Reveal in Finder/Folder' POSTs to /api/files/reveal
    with a folder_id body."""
    url = live_server["url"]
    page.goto(f"{url}/browse")

    item = page.locator(".tree-item[data-folder-id]").first
    item.wait_for(state="visible")
    item.click(button="right")
    menu = page.locator(".vireo-ctx-menu")
    expect(menu).to_be_visible()

    with page.expect_request(
        lambda r: r.url.endswith("/api/files/reveal") and r.method == "POST"
    ) as req_info:
        menu.locator(
            ".vireo-ctx-item", has_text="Reveal in"
        ).click()

    req = req_info.value
    body = req.post_data_json or {}
    assert "folder_id" in body, f"reveal request body missing folder_id: {body}"


def test_folder_tree_copy_path_fetches_folder(live_server, page):
    """Clicking 'Copy Path' fetches GET /api/folders/<id> to resolve the path."""
    url = live_server["url"]
    page.goto(f"{url}/browse")

    # Grant clipboard perms so the write call doesn't throw.
    page.context.grant_permissions(["clipboard-read", "clipboard-write"])

    item = page.locator(".tree-item[data-folder-id]").first
    item.wait_for(state="visible")
    fid = int(item.get_attribute("data-folder-id"))
    item.click(button="right")
    menu = page.locator(".vireo-ctx-menu")
    expect(menu).to_be_visible()

    with page.expect_response(
        lambda r: r.url.endswith(f"/api/folders/{fid}") and r.status == 200
    ):
        menu.locator(".vireo-ctx-item", has_text="Copy Path").click()


def test_folder_tree_right_click_does_not_trigger_filter(live_server, page):
    """Right-click must not fire the left-click onclick handler (filterByFolder).

    Regression guard: the folder tree items use inline onclick; a bare
    right-click must preventDefault and NOT also toggle the filter.
    """
    url = live_server["url"]
    page.goto(f"{url}/browse")

    item = page.locator(".tree-item[data-folder-id]").first
    item.wait_for(state="visible")
    assert page.evaluate("window.activeFolderId") in (None, 0)

    item.click(button="right")
    expect(page.locator(".vireo-ctx-menu")).to_be_visible()

    # activeFolderId should not have been mutated by the right-click itself.
    assert page.evaluate("window.activeFolderId") in (None, 0)
