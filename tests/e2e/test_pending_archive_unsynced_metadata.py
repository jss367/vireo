"""E2E: the "Photos kept locally" banner warns before a one-way transfer.

Once the NAS transfer verifies, Vireo deletes the local originals -- so any
metadata still queued in the sync panel can only ever be written over the
NAS connection afterwards, and only while that mount is up. The banner has
to say so before the click, and offer the cheap ordering.
"""

import os
from pathlib import Path

import pytest
from playwright.sync_api import expect
from services import photo_ingest, workspace_folders


def _staged_import(db, tmp_path, *, destination):
    """Seed a deferred import: staging folders, photos, and the archive row."""
    staging = tmp_path / "staging" / "import-kept-locally"
    day = staging / "2026" / "09" / "12"
    day.mkdir(parents=True)

    workspace_id = db.create_workspace("Kept Locally")
    day_id = workspace_folders.add_folder(db, str(day), name="12", link_to_workspace=False)
    workspace_folders.add_workspace_folder(db, workspace_id, day_id, is_root=True)
    photo_ids = [
        photo_ingest.add_photo(db, day_id, name, ".jpg", 1024, 0)
        for name in ("keep.jpg", "also-keep.jpg")
    ]
    db.conn.execute(
        "INSERT INTO pending_archives "
        "(id, workspace_id, destination, staging_destination, target_json) "
        "VALUES (?, ?, ?, ?, '{}')",
        ("import-kept-locally", workspace_id, destination, str(staging)),
    )
    db.conn.commit()
    return workspace_id, photo_ids


@pytest.mark.parametrize("sync_first", [False, True])
@pytest.mark.parametrize("entry", ["folder-menu", "badge-click", "badge-keyboard"])
def test_pending_folder_offers_nas_transfer_instead_of_another_local_copy(
    live_server, page, tmp_path, sync_first, entry
):
    db = live_server["db"]
    destination = "/Volumes/Photography/USA/2026"
    workspace_id, photo_ids = _staged_import(db, tmp_path, destination=destination)
    folder_id = db.photos.get(photo_ids[0])["folder_id"]
    if sync_first:
        db.queue_change(photo_ids[0], "rating", "3", workspace_id=workspace_id)
    assert page.request.post(
        f"{live_server['url']}/api/workspaces/{workspace_id}/activate"
    ).ok
    requests = []
    page.on("request", lambda request: requests.append(request.url))
    page.goto(f"{live_server['url']}/browse")
    item = page.locator(f'.tree-item[data-folder-id="{folder_id}"]')
    expect(item).to_contain_text("KEPT LOCALLY")
    original_url = page.url
    badge = item.get_by_role("button", name="NAS transfer options:")
    if entry == "folder-menu":
        item.click(button="right")
    elif entry == "badge-click":
        badge.click()
    else:
        badge.focus()
        badge.press("Enter")
    menu = page.locator(".vireo-ctx-menu")
    expect(menu.get_by_text("Work Locally…", exact=True)).to_have_count(0)
    if entry != "folder-menu":
        expect(badge).to_have_attribute("aria-expanded", "true")
        expect(menu.get_by_role("menuitem", name="Send to NAS…")).to_be_focused()
        assert page.url == original_url
        if directory := os.environ.get("VIREO_TRANSFER_SCREENSHOTS"):
            target = Path(directory)
            target.mkdir(parents=True, exist_ok=True)
            page.screenshot(path=str(target / "kept-locally-menu.png"), clip={
                "x": 0, "y": 80, "width": 330, "height": 230,
            })
    if entry == "badge-keyboard":
        menu.press("Escape")
        expect(menu).to_have_count(0)
        expect(badge).to_be_focused()
        expect(badge).to_have_attribute("aria-expanded", "false")
        badge.press("Space")
        menu.get_by_role("menuitem", name="Send to NAS…").press("Enter")
    else:
        menu.get_by_text("Send to NAS…", exact=True).click()
    expect(badge).to_have_attribute("aria-expanded", "false")

    banner = page.locator("#pendingArchives")
    expect(banner.locator("details")).to_have_attribute("open", "")
    expect(banner.locator(".pending-archive-destination")).to_have_text(
        "Destination: " + destination
    )
    expect(page.locator("#stageLocalFoldersModal")).not_to_have_class("modal-overlay open")
    label = "Sync metadata and send to NAS" if sync_first else "Send to NAS"
    send = banner.get_by_role("button", name=label, exact=True)
    expect(send).to_be_focused()
    assert not any("/local-folders/preflight" in url for url in requests)

    if directory := os.environ.get("VIREO_TRANSFER_SCREENSHOTS"):
        target = Path(directory)
        target.mkdir(parents=True, exist_ok=True)
        banner.screenshot(path=str(target / f"pending-folder-send-{sync_first}.png"))

    page.route("**/api/import/pending-archives/*/send", lambda route: route.fulfill(
        status=202, json={"job_id": "send-test"}
    ))
    with page.expect_request("**/api/import/pending-archives/*/send") as sent:
        send.click()
    assert sent.value.post_data_json == {"sync_first": sync_first}


def test_banner_offers_to_sync_metadata_before_sending(live_server, page, tmp_path):
    db = live_server["db"]
    workspace_id, photo_ids = _staged_import(
        db, tmp_path, destination="/Volumes/Photography/USA/2026")

    assert page.request.post(
        f"{live_server['url']}/api/workspaces/{workspace_id}/activate"
    ).ok
    page.goto(f"{live_server['url']}/browse")

    banner = page.locator("#pendingArchives")
    expect(banner).to_be_visible()
    # Nothing queued yet: the plain transfer is the only thing on offer, and
    # no notice invents a problem that does not exist.
    expect(banner.get_by_role("button", name="Send to NAS")).to_be_visible()
    expect(
        banner.get_by_role("button", name="Sync metadata and send to NAS")
    ).to_have_count(0)
    expect(banner).not_to_contain_text("not written to their sidecars")

    db.queue_change(photo_ids[0], "keyword_add", "Osprey", workspace_id=workspace_id)
    db.queue_change(photo_ids[1], "rating", "3", workspace_id=workspace_id)

    # The banner polls every 5s; the count has to name the photos, not the
    # queued rows, because "2 photos" is what the sentence claims.
    expect(banner).to_contain_text(
        "2 photos here have metadata changes that are not written to their "
        "sidecars yet",
        timeout=15000,
    )
    expect(banner).to_contain_text("after the transfer the same sync has to run over the NAS connection")

    sync_and_send = banner.get_by_role("button", name="Sync metadata and send to NAS")
    expect(sync_and_send).to_be_enabled()

    # Stub the transfer itself: this test is about which request the button
    # sends, and the seeded archive has no real NAS target behind it.
    sent = []

    def capture(route):
        sent.append(route.request.post_data_json)
        route.fulfill(status=200, content_type="application/json", body='{"job_id": 1}')

    page.route("**/api/import/pending-archives/*/send", capture)
    sync_and_send.click()
    page.wait_for_timeout(1000)
    assert sent == [{"sync_first": True}], sent

    # The plain transfer stays available: sending stale sidecars is a
    # legitimate choice, so this is a notice and not a gate.
    sent.clear()
    banner.get_by_role("button", name="Send to NAS", exact=True).click()
    page.wait_for_timeout(1000)
    assert sent == [{"sync_first": False}], sent


def test_banner_says_which_edits_this_sync_will_not_write(live_server, page, tmp_path):
    """The queue is per workspace, so this button cannot write a sibling's edits.

    Lumping them into one number would promise work that is not going to
    happen; they get their own sentence naming what to do instead.
    """
    db = live_server["db"]
    workspace_id, photo_ids = _staged_import(
        db, tmp_path, destination="/Volumes/Photography/USA/2026")
    sibling = db.create_workspace("Sibling")

    db.queue_change(photo_ids[0], "keyword_add", "Osprey", workspace_id=workspace_id)
    db.queue_change(photo_ids[1], "rating", "3", workspace_id=sibling)

    assert page.request.post(
        f"{live_server['url']}/api/workspaces/{workspace_id}/activate"
    ).ok
    page.goto(f"{live_server['url']}/browse")

    banner = page.locator("#pendingArchives")
    expect(banner).to_contain_text(
        "1 photo here has metadata changes that are not written to its sidecar yet",
        timeout=15000,
    )
    expect(banner).to_contain_text(
        "1 more photo has changes queued in another workspace"
    )
    expect(banner).to_contain_text("switch to that workspace and sync there first")
