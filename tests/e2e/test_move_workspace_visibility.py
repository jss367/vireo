"""The move choice names affected workspaces and survives a page restart."""

from playwright.sync_api import expect


def test_move_visibility_checkbox_remembers_choice_and_lists_workspaces(live_server, page):
    db = live_server["db"]
    a = db._active_workspace_id
    b = db.create_workspace("Other birds")
    photo = db.conn.execute("SELECT id, folder_id FROM photos ORDER BY id LIMIT 1").fetchone()
    db.add_workspace_folder(b, photo["folder_id"])
    page.goto(live_server["url"] + "/move")
    checkbox = page.get_by_label("Keep visible in other workspaces", exact=True)
    expect(checkbox).to_be_enabled()
    expect(checkbox).to_be_checked()
    page.evaluate("selectedPhotos.add(%d); updatePhotoSelectionUI();" % photo["id"])
    expect(page.locator("#moveAffectedWorkspaces")).to_contain_text("Other birds")
    expect(page.locator("#moveAffectedWorkspaces")).to_contain_text("Visibility will be kept")
    checkbox.uncheck()
    expect(checkbox).to_be_enabled()
    expect(page.locator("#moveAffectedWorkspaces")).to_contain_text("permits removal")
    page.reload()
    expect(checkbox).to_be_enabled()
    expect(checkbox).not_to_be_checked()
    assert db._active_workspace_id == a


def test_move_visibility_choice_is_not_silently_changed_when_save_fails(live_server, page):
    page.goto(live_server["url"] + "/move")
    checkbox = page.get_by_label("Keep visible in other workspaces", exact=True)
    expect(checkbox).to_be_enabled()
    expect(checkbox).to_be_checked()
    def fail_write(route):
        if route.request.method == "POST":
            route.fulfill(status=500, content_type="application/json", body='{"error":"save failed"}')
        else:
            route.continue_()
    page.route("**/api/config", fail_write)
    checkbox.uncheck()
    expect(checkbox).to_be_checked()
    expect(checkbox).to_be_enabled()
    page.reload()
    expect(checkbox).to_be_checked()
