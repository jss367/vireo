"""E2E test: /duplicates page restores prior scan results on mount.

Without restore, navigating away from /duplicates and back loses the
in-memory proposals and forces a fresh scan. The page now hydrates from
the most recent completed ``duplicate-scan`` row in ``job_history``.
"""
import json

from playwright.sync_api import expect


def _seed_prior_scan(db, stale_group=False, only_stale=False):
    """Insert a completed duplicate-scan into job_history over real rows.

    last-scan drops groups whose ids no longer name the same catalog file,
    so the restored group must point at photos that exist. ``stale_group``
    adds a second group whose ids name no photo. ``only_stale`` seeds a
    single group whose ids name no photo and no live groups at all, so
    revalidation drops every proposal.
    """
    proposals = []
    if not only_stale:
        fid = db.add_folder("/photos")
        ids = [
            db.add_photo(folder_id=fid, filename=name, extension=".jpg",
                         file_size=1000, file_mtime=100.0, file_hash="HFAKE")
            for name in ("a.jpg", "a (2).jpg")
        ]
        db.conn.execute("UPDATE photos SET flag='none' WHERE file_hash='HFAKE'")
        proposals.append({
            "file_hash": "HFAKE",
            "status": "unresolved",
            "winner": {"id": ids[0], "filename": "a.jpg", "path": "/photos/a.jpg", "file_size": 1000},
            "losers": [
                {"id": ids[1], "filename": "a (2).jpg", "path": "/photos/a (2).jpg", "file_size": 1000}
            ],
        })
    if stale_group or only_stale:
        proposals.append({
            "file_hash": "HGONE",
            "status": "resolved",
            "winner": {"id": 9001, "filename": "b.jpg", "path": "/photos/b.jpg", "file_size": 1000},
            "losers": [
                {"id": 9002, "filename": "b-2.jpg", "path": "/photos/b-2.jpg", "file_size": 1000}
            ],
        })
    result = {
        "group_count": len(proposals),
        "loser_count": 1,
        "proposals": proposals,
    }
    db.conn.execute(
        """INSERT INTO job_history
              (id, type, status, started_at, finished_at, duration, result)
           VALUES (?, 'duplicate-scan', 'completed', ?, ?, 1.0, ?)""",
        (
            "duplicate-scan-restore-test",
            "2026-04-27T19:00:00",
            "2026-04-27T19:00:01",
            json.dumps(result),
        ),
    )
    db.conn.commit()


def test_duplicates_page_restores_prior_scan(live_server, page):
    """A prior completed scan in job_history rehydrates the page on mount."""
    _seed_prior_scan(live_server["db"])
    page.goto(f"{live_server['url']}/duplicates")

    banner = page.locator("#restoredBanner")
    expect(banner).to_be_visible()
    expect(banner).to_contain_text("Showing results from your last scan")
    expect(page.locator("#emptyState")).not_to_be_visible()
    expect(page.locator("#results")).to_contain_text("HFAKE")
    expect(banner).not_to_contain_text("no longer")
    expect(banner).to_contain_text("Still up to date")
    expect(banner).not_to_contain_text("Scan again")


def test_duplicates_page_says_when_duplicates_appeared_since_the_scan(
    live_server, page,
):
    """A group imported after the restored scan is something only a new scan
    shows, so the banner asks for one instead of calling the scan current."""
    db = live_server["db"]
    _seed_prior_scan(db)
    fid = db.add_folder("/later")
    for name in ("c.jpg", "c (2).jpg"):
        db.add_photo(folder_id=fid, filename=name, extension=".jpg",
                     file_size=1000, file_mtime=100.0, file_hash="HLATER")
    db.conn.execute("UPDATE photos SET flag='none' WHERE file_hash='HLATER'")
    db.conn.commit()
    page.goto(f"{live_server['url']}/duplicates")

    banner = page.locator("#restoredBanner")
    expect(banner).to_contain_text(
        "Since then, 1 new duplicate group has appeared. Scan again"
    )
    expect(banner).not_to_contain_text("Still up to date")


def test_duplicates_page_hides_restored_groups_that_no_longer_match(
    live_server, page,
):
    """A restored group whose photos are gone is hidden, and the banner
    says so instead of showing cards for photos the ids no longer name."""
    _seed_prior_scan(live_server["db"], stale_group=True)
    page.goto(f"{live_server['url']}/duplicates")

    banner = page.locator("#restoredBanner")
    expect(banner).to_contain_text(
        "1 group from that scan no longer matches your catalog and is hidden."
    )
    expect(page.locator("#results")).to_contain_text("HFAKE")
    expect(page.locator("#results")).not_to_contain_text("b-2.jpg")


def test_duplicates_page_all_stale_still_a_group_asks_for_a_rescan(
    live_server, page,
):
    """When every restored group's photos are gone but a stale group's hash
    still forms a group in the catalog, the empty state asks for a fresh scan
    instead of claiming the library is clean."""
    db = live_server["db"]
    _seed_prior_scan(db, only_stale=True)
    fid = db.add_folder("/photos")
    for name in ("b.jpg", "b (2).jpg"):
        db.add_photo(folder_id=fid, filename=name, extension=".jpg",
                     file_size=1000, file_mtime=100.0, file_hash="HGONE")
    db.conn.execute("UPDATE photos SET flag='none' WHERE file_hash='HGONE'")
    db.conn.commit()
    page.goto(f"{live_server['url']}/duplicates")

    results = page.locator("#results")
    expect(results).to_contain_text(
        "The last scan is out of date — 1 group from it has changed since."
    )
    expect(results).not_to_contain_text("Your library is clean")
    expect(page.locator("#restoredBanner")).to_contain_text(
        "1 group from that scan has changed"
    )
    # The banner's stale text would duplicate the empty-state message, so
    # it is suppressed when nothing remains to show alongside it.
    expect(page.locator("#restoredStale")).to_have_text("")


def test_duplicates_page_all_stale_with_no_duplicates_left_is_clean(
    live_server, page,
):
    """When every restored group is gone and the catalog has no duplicate
    groups, a new scan would find none, so the banner and the empty state
    agree that nothing needs a rescan."""
    _seed_prior_scan(live_server["db"], only_stale=True)
    page.goto(f"{live_server['url']}/duplicates")

    expect(page.locator("#results")).to_contain_text("No duplicates found")
    expect(page.locator("#results")).not_to_contain_text("out of date")
    expect(page.locator("#restoredBanner")).to_contain_text("Still up to date")


def test_duplicates_page_empty_restore_names_duplicates_added_since(
    live_server, page,
):
    """A restored scan with nothing left to show is not a clean bill of
    health when duplicates have been imported since it ran."""
    db = live_server["db"]
    _seed_prior_scan(db, only_stale=True)
    fid = db.add_folder("/later")
    for name in ("c.jpg", "c (2).jpg"):
        db.add_photo(folder_id=fid, filename=name, extension=".jpg",
                     file_size=1000, file_mtime=100.0, file_hash="HLATER")
    db.conn.execute("UPDATE photos SET flag='none' WHERE file_hash='HLATER'")
    db.conn.commit()
    page.goto(f"{live_server['url']}/duplicates")

    results = page.locator("#results")
    expect(results).to_contain_text(
        "1 new duplicate group has appeared since. Run a new scan to see it."
    )
    expect(results).not_to_contain_text("Your library is clean")


def test_duplicates_page_no_prior_scan_shows_empty_state(live_server, page):
    """No prior scan -> banner hidden, empty-state visible."""
    page.goto(f"{live_server['url']}/duplicates")

    expect(page.locator("#restoredBanner")).not_to_be_visible()
    expect(page.locator("#emptyState")).to_be_visible()


def test_duplicates_page_starting_new_scan_hides_banner(live_server, page):
    """Clicking 'Scan' clears the restored banner."""
    _seed_prior_scan(live_server["db"])
    page.goto(f"{live_server['url']}/duplicates")
    expect(page.locator("#restoredBanner")).to_be_visible()

    page.click("#scanBtn")
    expect(page.locator("#restoredBanner")).not_to_be_visible()


def _seed_catalog_cleanup(db):
    fid = db.add_folder("/photos/cleanup")
    ids = [
        db.add_photo(folder_id=fid, filename=name, extension=".jpg",
                     file_size=1000, file_mtime=100.0, file_hash="HCLEANUP")
        for name in ("kept.jpg", "extra.jpg")
    ]
    db.conn.execute("UPDATE photos SET flag='none' WHERE id=?", (ids[0],))
    db.conn.execute("UPDATE photos SET flag='rejected' WHERE id=?", (ids[1],))
    db.conn.commit()
    return ids


def test_cleanup_banner_opens_current_copies_without_a_saved_scan(live_server, page):
    _seed_catalog_cleanup(live_server["db"])
    page.goto(f"{live_server['url']}/browse")
    expect(page.locator("#dupCleanupMsg")).to_contain_text("1 duplicate file copy could be cleaned up")
    page.locator("#dupCleanupBanner a").click()

    expect(page.locator("#catalogBanner")).to_be_visible()
    expect(page.locator("#resolvedList")).to_be_visible()
    expect(page.locator("#results")).to_contain_text("extra.jpg")
    expect(page.locator("#emptyState")).not_to_be_visible()
    expect(page.locator("#restoredBanner")).not_to_be_visible()
    expect(page.locator("#trashAllBtn")).to_have_text("Move 1 extra copy to Trash")


def test_cleanup_link_uses_current_catalog_instead_of_an_old_scan(live_server, page):
    _seed_prior_scan(live_server["db"])
    _seed_catalog_cleanup(live_server["db"])
    page.goto(f"{live_server['url']}/duplicates?show=resolved")

    expect(page.locator("#results")).to_contain_text("extra.jpg")
    expect(page.locator("#results")).not_to_contain_text("HFAKE")
    expect(page.locator("#restoredBanner")).not_to_be_visible()


def test_normal_duplicates_page_also_finds_cleanup_without_history(live_server, page):
    _seed_catalog_cleanup(live_server["db"])
    page.goto(f"{live_server['url']}/duplicates")
    expect(page.locator("#catalogBanner")).to_be_visible()
    expect(page.locator("#resolvedList")).to_be_visible()
    expect(page.locator("#emptyState")).not_to_be_visible()


def test_duplicates_loading_does_not_ask_for_a_scan(live_server, page):
    _seed_catalog_cleanup(live_server["db"])
    pending = []
    page.route("**/api/duplicates/cleanup", lambda route: pending.append(route))
    page.goto(f"{live_server['url']}/duplicates?show=resolved", wait_until="domcontentloaded")

    expect(page.locator("#initialLoading")).to_be_visible()
    expect(page.locator("#emptyState")).not_to_be_visible()
    expect(page.locator("#scanBtn")).to_be_disabled()
    assert len(pending) == 1
    pending[0].fulfill(response=pending[0].fetch())
    expect(page.locator("#results")).to_contain_text("extra.jpg")
    expect(page.locator("#initialLoading")).not_to_be_visible()
    expect(page.locator("#scanBtn")).to_be_enabled()


def test_cleanup_load_failure_is_retryable(live_server, page):
    _seed_catalog_cleanup(live_server["db"])
    attempts = []

    def respond(route):
        attempts.append(route)
        if len(attempts) == 1:
            route.fulfill(status=500, json={"error": "Temporary failure"})
        else:
            route.continue_()

    page.route("**/api/duplicates/cleanup", respond)
    page.goto(f"{live_server['url']}/duplicates?show=resolved")
    expect(page.locator("#loadError")).to_contain_text("Could not load duplicate results")
    expect(page.locator("#emptyState")).not_to_be_visible()
    expect(page.locator("#initialLoading")).not_to_be_visible()
    page.locator("#loadError button").click()
    expect(page.locator("#results")).to_contain_text("extra.jpg")
    expect(page.locator("#loadError")).not_to_be_visible()


def test_cleanup_link_explains_when_copies_are_no_longer_pending(live_server, page):
    page.goto(f"{live_server['url']}/duplicates?show=resolved")
    expect(page.locator("#results")).to_contain_text("No rejected duplicate copies are pending cleanup")
    expect(page.locator("#results")).not_to_contain_text("Your library is clean")
    expect(page.locator("#emptyState")).not_to_be_visible()
    expect(page.locator("#scanBtn")).to_be_enabled()


def test_cleared_results_cannot_be_revived_by_a_late_response(live_server, page):
    _seed_catalog_cleanup(live_server["db"])
    pending = []
    page.route("**/api/duplicates/cleanup", lambda route: pending.append(route))
    page.goto(f"{live_server['url']}/duplicates?show=resolved", wait_until="domcontentloaded")
    expect(page.locator("#initialLoading")).to_be_visible()
    assert len(pending) == 1
    response = pending[0].fetch()
    page.evaluate("clearResults()")
    pending[0].fulfill(response=response)
    # Clearing the loading state also keeps the scan control usable.
    page.wait_for_function("document.getElementById('scanBtn').disabled === false")
    expect(page.locator("#emptyState")).to_be_visible()
    expect(page.locator("#results")).to_be_empty()
    expect(page.locator("#catalogBanner")).not_to_be_visible()


def test_cleanup_refresh_drops_a_rejection_undone_elsewhere(live_server, page):
    _, rejected = _seed_catalog_cleanup(live_server["db"])
    page.goto(f"{live_server['url']}/duplicates?show=resolved")
    expect(page.locator("#results")).to_contain_text("extra.jpg")
    db = live_server["db"]
    db.conn.execute("UPDATE photos SET flag='none' WHERE id=?", (rejected,))
    db.conn.commit()
    page.locator("#catalogBanner button").click()
    expect(page.locator("#results")).to_contain_text("No rejected duplicate copies are pending cleanup")
    expect(page.locator("#trashAllBtn")).to_have_count(0)


def test_slow_thumbnails_do_not_block_cleanup_and_have_visible_states(live_server, page):
    import io

    from PIL import Image

    _seed_catalog_cleanup(live_server["db"])
    pending = []
    page.route("**/thumbnails/duplicate/**", lambda route: pending.append(route))
    page.goto(f"{live_server['url']}/duplicates?show=resolved", wait_until="domcontentloaded")
    expect(page.locator("#results")).to_contain_text("extra.jpg")
    expect(page.locator("#initialLoading")).not_to_be_visible()
    expect(page.locator("#trashAllBtn")).to_be_enabled()
    expect(page.locator(".thumb-wrap .thumb-placeholder").first).to_have_text("Loading thumbnail…")
    image = io.BytesIO()
    Image.new("RGB", (180, 135), color="green").save(image, format="JPEG")
    assert len(pending) == 2
    pending[0].fulfill(content_type="image/jpeg", body=image.getvalue())
    expect(page.locator(".thumb-wrap.loaded")).to_have_count(1)
    pending[1].abort()
    expect(page.locator(".thumb-wrap:not(.loaded) .thumb-placeholder")).to_have_text("No thumbnail")


def test_distant_duplicate_thumbnails_wait_until_scrolled_into_view(live_server, page):
    _seed_catalog_cleanup(live_server["db"])
    requests = []
    page.on("request", lambda request: requests.append(request.url))
    page.goto(f"{live_server['url']}/duplicates?show=resolved")
    expect(page.locator("#catalogBanner")).to_be_visible()
    page.evaluate("""() => {
      document.getElementById('results').innerHTML = '<div style="height:20000px"></div>' +
        renderCard({id: 999999, filename: 'distant.jpg'}, true, null, false);
    }""")
    distant = page.locator('img[alt="distant.jpg"]')
    expect(distant).to_have_attribute("loading", "lazy")
    assert not any("/thumbnails/duplicate/999999.jpg" in url for url in requests)
    with page.expect_request("**/thumbnails/duplicate/999999.jpg"):
        distant.scroll_into_view_if_needed()
