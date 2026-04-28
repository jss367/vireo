"""E2E test: /duplicates page renders the bulk-decide section when the
scan result includes multi-group buckets, and a single Keep-folder click
resolves all groups in the bucket.
"""
import json

from playwright.sync_api import expect


def _seed_scan_with_buckets(db, folder_a, folder_b, n_groups=3):
    """Insert a synthetic scan result with one bucket of N groups, all
    sharing the same {folder_a, folder_b} parent-dir set."""
    # Need real DB rows so the bulk-resolve API can find candidates.
    a_fid = db.add_folder(folder_a)
    b_fid = db.add_folder(folder_b)
    proposals = []
    file_hashes = []
    for i in range(n_groups):
        h = f"BULK{i}"
        name = f"photo{i}.jpg"
        p_a = db.add_photo(folder_id=a_fid, filename=name, extension=".jpg",
                           file_size=1000, file_mtime=100.0, file_hash=h)
        p_b = db.add_photo(folder_id=b_fid, filename=name, extension=".jpg",
                           file_size=1000, file_mtime=100.0, file_hash=h)
        db.conn.execute("UPDATE photos SET flag='none' WHERE file_hash=?", (h,))
        proposals.append({
            "file_hash": h,
            "status": "unresolved",
            "winner": {"id": p_a, "filename": name,
                       "path": f"{folder_a}/{name}", "file_size": 1000},
            "losers": [{"id": p_b, "filename": name,
                        "path": f"{folder_b}/{name}", "file_size": 1000,
                        "reason": "shorter path"}],
        })
        file_hashes.append(h)
    db.conn.commit()

    result = {
        "group_count": n_groups,
        "loser_count": n_groups,
        "proposals": proposals,
        "buckets": [{
            "folders": sorted([folder_a, folder_b]),
            "group_count": n_groups,
            "file_hashes": file_hashes,
            "total_size": n_groups * 1000,
            "example_filenames": [p["winner"]["filename"] for p in proposals[:3]],
        }],
    }
    db.conn.execute(
        """INSERT INTO job_history
              (id, type, status, started_at, finished_at, duration, result)
           VALUES (?, 'duplicate-scan', 'completed', ?, ?, 1.0, ?)""",
        (
            "duplicate-scan-bulk-test",
            "2026-04-27T19:00:00",
            "2026-04-27T19:00:01",
            json.dumps(result),
        ),
    )
    db.conn.commit()
    return file_hashes, [p["winner"]["id"] for p in proposals], \
        [p["losers"][0]["id"] for p in proposals]


def test_duplicates_page_renders_bulk_decide_section(live_server, page):
    """A bucket with 3 groups surfaces a 'Bulk decide' section with one
    'Keep ... for all 3' button per folder."""
    folder_a, folder_b = "/tmp/dupbulkdecideA", "/tmp/dupbulkdecideB"
    _seed_scan_with_buckets(live_server["db"], folder_a, folder_b, n_groups=3)

    page.goto(f"{live_server['url']}/duplicates")

    expect(page.locator("h2", has_text="Bulk decide")).to_be_visible()
    bucket = page.locator(".bucket-card").first
    expect(bucket).to_contain_text("3")
    expect(bucket).to_contain_text(folder_a)
    expect(bucket).to_contain_text(folder_b)
    # One button per folder, labeled with the folder's basename.
    keep_buttons = bucket.locator(".keep-btn")
    expect(keep_buttons).to_have_count(2)
    expect(keep_buttons.nth(0)).to_contain_text("for all 3")


def test_duplicates_bulk_decide_keep_folder_resolves_all_groups(live_server, page):
    """Clicking 'Keep <folder> for all N' POSTs to bulk-resolve, flips the
    DB state, and removes the bucket from the rendered page."""
    folder_a, folder_b = "/tmp/dupbulkactA", "/tmp/dupbulkactB"
    file_hashes, winner_ids, loser_ids = _seed_scan_with_buckets(
        live_server["db"], folder_a, folder_b, n_groups=2
    )

    # Auto-accept the confirm() dialog. Wired before navigation so any
    # early dialog is also handled.
    page.on("dialog", lambda d: d.accept())
    page.goto(f"{live_server['url']}/duplicates")
    expect(page.locator(".bucket-card")).to_have_count(1)

    # Pick the /a button (index 0; folders are sorted alphabetically).
    page.locator(".keep-btn").first.click()

    # Bucket gone post-resolution.
    expect(page.locator(".bucket-card")).to_have_count(0)

    # DB confirms /a winners kept, /b siblings rejected.
    db = live_server["db"]
    flags = {
        r["id"]: r["flag"]
        for r in db.conn.execute(
            f"SELECT id, flag FROM photos WHERE id IN ({','.join('?' * (len(winner_ids) + len(loser_ids)))})",
            winner_ids + loser_ids,
        ).fetchall()
    }
    for wid in winner_ids:
        assert flags[wid] == "none", f"winner {wid}"
    for lid in loser_ids:
        assert flags[lid] == "rejected", f"loser {lid}"
