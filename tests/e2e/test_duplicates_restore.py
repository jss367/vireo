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


def test_duplicates_page_all_stale_does_not_declare_library_clean(
    live_server, page,
):
    """When every restored group's photos are gone, the empty state asks
    for a fresh scan instead of claiming the library is clean — the old
    scan only tells us its groups are out of date."""
    _seed_prior_scan(live_server["db"], only_stale=True)
    page.goto(f"{live_server['url']}/duplicates")

    results = page.locator("#results")
    expect(results).to_contain_text("The last scan is out of date")
    expect(results).to_contain_text("Run a new scan")
    expect(results).not_to_contain_text("Your library is clean")
    # The banner's stale text would duplicate the empty-state message, so
    # it is suppressed when nothing remains to show alongside it.
    expect(page.locator("#restoredBanner")).to_be_visible()
    expect(page.locator("#restoredStale")).to_have_text("")


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
