"""E2E: the "Photos kept locally" banner warns before a one-way transfer.

Once the NAS transfer verifies, Vireo deletes the local originals -- so any
metadata still queued in the sync panel can only ever be written over the
NAS connection afterwards, and only while that mount is up. The banner has
to say so before the click, and offer the cheap ordering.
"""

from playwright.sync_api import expect


def _staged_import(db, tmp_path, *, destination):
    """Seed a deferred import: staging folders, photos, and the archive row."""
    staging = tmp_path / "staging" / "import-kept-locally"
    day = staging / "2026" / "09" / "12"
    day.mkdir(parents=True)

    workspace_id = db.create_workspace("Kept Locally")
    day_id = db.add_folder(str(day), name="12", link_to_workspace=False)
    db.add_workspace_folder(workspace_id, day_id, is_root=True)
    photo_ids = [
        db.add_photo(day_id, name, ".jpg", 1024, 0)
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
