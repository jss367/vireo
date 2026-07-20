from pathlib import Path

from playwright.sync_api import expect


def test_work_locally_full_cycle(live_server, page, tmp_path):
    """Stage a workspace locally, edit a file, and sync it back — driving
    only the UI: buttons, confirm dialogs, and the live progress panel."""
    db = live_server["db"]
    source = tmp_path / "nas-src"
    source.mkdir()
    (source / "bird.jpg").write_bytes(b"original-bytes")

    ws_id = db.create_workspace("Local Cycle")
    db.set_active_workspace(ws_id)
    folder_id = db.add_folder(str(source), name="nas-src")

    activate = page.request.post(f"{live_server['url']}/api/workspaces/{ws_id}/activate")
    assert activate.ok

    page.on("dialog", lambda dialog: dialog.accept())
    page.goto(f"{live_server['url']}/workspace", timeout=5000)

    work_locally = page.get_by_role("button", name="Work Locally", exact=True)
    expect(work_locally).to_be_visible(timeout=5000)
    work_locally.click()

    panel = page.locator("#localWorkspaceContent")
    expect(panel).to_contain_text("using local storage", timeout=15000)

    local_path = db.conn.execute("SELECT path FROM folders WHERE id=?", (folder_id,)).fetchone()["path"]
    assert local_path != str(source)
    Path(local_path, "bird.jpg").write_bytes(b"edited-locally")

    page.get_by_role("button", name="Sync Back", exact=True).click()
    expect(page.get_by_role("button", name="Work Locally", exact=True)).to_be_visible(timeout=15000)

    assert (source / "bird.jpg").read_bytes() == b"edited-locally"
    restored = db.conn.execute("SELECT path FROM folders WHERE id=?", (folder_id,)).fetchone()["path"]
    assert restored == str(source)


def test_workspace_page_lists_all_workspaces(live_server, page):
    """Workspace page shows both Default and Field Work workspaces."""
    url = live_server["url"]
    page.goto(f"{url}/workspace", timeout=5000)

    container = page.locator("#workspacesContent")

    # Workspace names are rendered as <input> elements inside the container.
    # Wait for the async JS fetch to populate the list.
    default_input = container.locator("input[value='Default']")
    field_work_input = container.locator("input[value='Field Work']")

    expect(default_input).to_be_visible(timeout=5000)
    expect(field_work_input).to_be_visible(timeout=5000)
    expect(page.get_by_role("button", name="Work Locally", exact=True).first).to_be_visible()


def test_shared_folder_local_status_follows_workspace_switch(live_server, page, tmp_path):
    """One managed copy is visible and controllable from every linked workspace."""
    db = live_server["db"]
    source = tmp_path / "shared-source"
    source.mkdir()
    (source / "bird.jpg").write_bytes(b"original")
    first = db.create_workspace("Shared First")
    second = db.create_workspace("Shared Second")
    folder_id = db.add_folder(str(source), name="shared-source", link_to_workspace=False)
    db.add_workspace_folder(first, folder_id)
    db.add_workspace_folder(second, folder_id)

    page.on("dialog", lambda dialog: dialog.accept())
    assert page.request.post(f"{live_server['url']}/api/workspaces/{first}/activate").ok
    page.goto(f"{live_server['url']}/workspace", timeout=5000)
    page.get_by_role("button", name="Work Locally", exact=True).click()
    expect(page.get_by_text("Local · 2 workspaces", exact=True)).to_be_visible(timeout=15000)

    local_row = page.locator(".workspace-folder-row-stacked")
    expect(local_row).to_have_count(1)
    main_box = local_row.locator(".workspace-folder-main").bounding_box()
    actions_box = local_row.locator(".workspace-folder-actions").bounding_box()
    assert main_box is not None
    assert actions_box is not None
    assert actions_box["y"] >= main_box["y"] + main_box["height"]

    assert page.request.post(f"{live_server['url']}/api/workspaces/{second}/activate").ok
    page.reload(timeout=5000)
    expect(page.get_by_text("Local · 2 workspaces", exact=True)).to_be_visible(timeout=5000)
    page.get_by_role("button", name="Discard", exact=True).click()
    expect(page.get_by_role("button", name="Work Locally", exact=True)).to_be_visible(timeout=15000)
    assert (source / "bird.jpg").read_bytes() == b"original"
