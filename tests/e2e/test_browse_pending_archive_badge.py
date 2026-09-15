"""E2E: Browse says which sidebar folders are still only kept locally.

An import that deferred its NAS transfer registers Vireo's staging tree as
ordinary catalog folders, so the sidebar row can be a bare date leaf ("12")
that looks exactly like a folder already sitting on the destination storage.
The row has to say otherwise.
"""

import re

from playwright.sync_api import expect


def _pending_archive(db, workspace_id, staging, destination):
    db.conn.execute(
        "INSERT INTO pending_archives "
        "(id, workspace_id, destination, staging_destination, target_json) "
        "VALUES (?, ?, ?, ?, '{}')",
        ("import-kept-locally", workspace_id, destination, str(staging)),
    )
    db.conn.commit()


def test_browse_badges_folders_waiting_for_transfer(live_server, page, tmp_path):
    db = live_server["db"]
    staging = tmp_path / "staging" / "import-kept-locally" / "2026"
    day = staging / "09" / "12"
    day.mkdir(parents=True)
    elsewhere = tmp_path / "already-transferred"
    elsewhere.mkdir()

    workspace_id = db.create_workspace("Kept Locally")
    day_id = db.add_folder(str(day), name="12", link_to_workspace=False)
    elsewhere_id = db.add_folder(
        str(elsewhere), name="already-transferred", link_to_workspace=False
    )
    db.add_workspace_folder(workspace_id, day_id, is_root=True)
    db.add_workspace_folder(workspace_id, elsewhere_id, is_root=True)
    _pending_archive(db, workspace_id, staging, "/Volumes/Photography/USA/2026")

    assert page.request.post(
        f"{live_server['url']}/api/workspaces/{workspace_id}/activate"
    ).ok
    page.goto(f"{live_server['url']}/browse")

    row = page.locator(f'.tree-item[data-folder-id="{day_id}"]')
    row.wait_for(state="visible")
    badge = row.locator(".folder-archive-status-slot .folder-local-status")
    expect(badge).to_have_text("KEPT LOCALLY", timeout=10000)
    # The badge has to name where the files still have to go, or it is just
    # another opaque label.
    expect(badge).to_have_attribute(
        "aria-label", re.compile(re.escape("/Volumes/Photography/USA/2026"))
    )
    # Hovering the name resolves "12" to the directory it actually is.
    expect(row.locator(".folder-name")).to_have_attribute("title", str(day))

    other = page.locator(f'.tree-item[data-folder-id="{elsewhere_id}"]')
    expect(
        other.locator(".folder-archive-status-slot .folder-local-status")
    ).to_have_count(0)

    # Once the transfer is gone the badge has to go with it — a folder that
    # still reads KEPT LOCALLY after its photos reached the NAS is the same
    # lie in the other direction. The open page picks this up from its own
    # poll, with no reload.
    db.conn.execute("DELETE FROM pending_archives WHERE id = 'import-kept-locally'")
    db.conn.commit()
    expect(badge).to_have_count(0, timeout=15000)
