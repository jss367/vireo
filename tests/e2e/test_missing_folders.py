from playwright.sync_api import expect


def _open_missing_folders(live_server, page):
    page.goto(f"{live_server['url']}/browse")
    page.evaluate("openMissingFoldersModal()")
    button = page.get_by_role("button", name="Remove All", exact=True)
    expect(button).to_be_visible()
    return button


def test_remove_all_missing_folders(live_server, page, tmp_path):
    db = live_server["db"]
    parent = live_server["data"]["folders"][0]
    child = db.add_folder("/photos/park/nested", name="nested", parent_id=parent)
    db.add_photo(child, "nested.jpg", ".jpg", 1000, 1.0)
    existing = tmp_path / "existing"
    existing.mkdir()
    original = existing / "keep.jpg"
    original.write_bytes(b"original")
    healthy = db.add_folder(str(existing), name="existing")
    healthy_photo = db.add_photo(healthy, original.name, ".jpg", 8, 1.0)

    button = _open_missing_folders(live_server, page)
    expect(page.locator("#missingFoldersList .missing-folder-row")).to_have_count(3)
    confirmations = []

    def accept(dialog):
        confirmations.append(dialog.message)
        dialog.accept()

    page.on("dialog", accept)
    button.click()
    expect(page.locator("#missingFoldersList")).to_have_text("✓All folders are accounted for.")
    expect(button).to_be_hidden()
    assert len(confirmations) == 1
    assert "3 missing folders" in confirmations[0]
    assert "6 photos" in confirmations[0]
    assert "won't be touched" in confirmations[0]
    assert [row[0] for row in db.conn.execute("SELECT id FROM folders")] == [healthy]
    assert [row[0] for row in db.conn.execute("SELECT id FROM photos")] == [healthy_photo]
    assert original.read_bytes() == b"original"


def test_cancel_remove_all_missing_folders(live_server, page):
    button = _open_missing_folders(live_server, page)
    page.on("dialog", lambda dialog: dialog.dismiss())
    button.click()
    expect(page.locator("#missingFoldersList .missing-folder-row")).to_have_count(2)
    expect(button).to_be_enabled()
    assert live_server["db"].conn.execute("SELECT COUNT(*) FROM photos").fetchone()[0] == 5


def test_remove_all_failure_refreshes_remaining_folders(live_server, page):
    button = _open_missing_folders(live_server, page)
    failed_id = live_server["data"]["folders"][1]
    page.route(
        f"**/api/folders/{failed_id}",
        lambda route: route.fulfill(
            status=409, json={"error": "This folder has a shared local copy."}
        ),
    )
    page.on("dialog", lambda dialog: dialog.accept())
    button.click()
    expect(page.locator("#missingFoldersList .missing-folder-row")).to_have_count(1)
    expect(button).to_be_enabled()
    expect(page.locator("#missingFoldersList")).to_contain_text("/photos/yard")
    assert live_server["db"].conn.execute("SELECT COUNT(*) FROM photos").fetchone()[0] == 2

    page.unroute(f"**/api/folders/{failed_id}")
    button.click()
    expect(page.locator("#missingFoldersList")).to_have_text("✓All folders are accounted for.")
    expect(button).to_be_hidden()
