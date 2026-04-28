"""E2E test: /duplicates page restores prior scan results on mount.

Without restore, navigating away from /duplicates and back loses the
in-memory proposals and forces a fresh scan. The page now hydrates from
the most recent completed ``duplicate-scan`` row in ``job_history``.
"""
import json

from playwright.sync_api import expect


def _seed_prior_scan(db):
    """Insert a synthetic completed duplicate-scan into job_history."""
    result = {
        "group_count": 1,
        "loser_count": 1,
        "proposals": [
            {
                "file_hash": "HFAKE",
                "status": "unresolved",
                "winner": {"id": 1, "filename": "a.jpg", "path": "/photos/a.jpg"},
                "losers": [
                    {"id": 2, "filename": "a (2).jpg", "path": "/photos/a (2).jpg"}
                ],
            }
        ],
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
