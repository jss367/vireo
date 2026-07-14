"""E2E coverage for visible, batched duplicate-file trash progress."""

import json

from playwright.sync_api import expect


def _seed_resolved_scan(db, loser_count):
    proposals = []
    for i in range(loser_count):
        proposals.append({
            "file_hash": f"RESOLVED{i}",
            "status": "resolved",
            "winner": {
                "id": 10_000 + i,
                "filename": f"kept-{i}.jpg",
                "path": f"/photos/kept-{i}.jpg",
                "file_size": 1000,
            },
            "losers": [{
                "id": 20_000 + i,
                "filename": f"extra-{i}.jpg",
                "path": f"/photos/extra-{i}.jpg",
                "file_size": 1000,
                "reason": "exact duplicate",
            }],
        })

    result = {
        "group_count": 0,
        "loser_count": 0,
        "resolved_group_count": loser_count,
        "resolved_loser_count": loser_count,
        "proposals": proposals,
    }
    db.conn.execute(
        """INSERT INTO job_history
              (id, type, status, started_at, finished_at, duration, result)
           VALUES (?, 'duplicate-scan', 'completed', ?, ?, 1.0, ?)""",
        (
            "duplicate-trash-progress-test",
            "2026-07-13T10:00:00",
            "2026-07-13T10:00:01",
            json.dumps(result),
        ),
    )
    db.conn.commit()


def test_trash_all_reports_progress_and_uses_small_batches(live_server, page):
    """A large cleanup exposes progress and never sends one giant request."""
    _seed_resolved_scan(live_server["db"], loser_count=55)
    requested_batches = []

    def handle_trash(route):
        ids = route.request.post_data_json["photo_ids"]
        requested_batches.append(ids)
        route.fulfill(
            status=200,
            content_type="application/json",
            body=json.dumps({
                "ok": True,
                "trashed": len(ids),
                "skipped": [],
                "failed": [],
            }),
        )

    page.route("**/api/duplicates/delete-loser-files", handle_trash)
    page.on("dialog", lambda dialog: dialog.accept())
    page.goto(f"{live_server['url']}/duplicates")

    expect(page.locator("#trashAllBtn")).to_contain_text("Move 55 extra copies")
    page.locator("#trashAllBtn").click()

    progress = page.locator("#trashAllProgress")
    expect(progress).to_be_visible()
    expect(progress).to_contain_text("Cleanup complete")
    expect(progress).to_contain_text("55 processed")
    expect(progress).to_contain_text("55 moved")
    expect(progress.locator("[role='progressbar']")).to_have_attribute(
        "aria-valuenow", "55",
    )
    expect(page.locator(".dup-card.trashed")).to_have_count(55)
    expect(page.locator("#trashAllBtn")).to_have_count(0)

    assert [len(batch) for batch in requested_batches] == [25, 25, 5]
    assert [pid for batch in requested_batches for pid in batch] == [
        20_000 + i for i in range(55)
    ]


def test_trash_progress_excludes_file_already_missing_from_skipped_count(
    live_server, page
):
    """A ``file already missing`` skip is a terminal DB-only completion —
    the card renders "Cleaned up", not "Skipped". The progress counter
    must agree: it must not fold those entries into the visible
    ``skipped`` count. Otherwise a cleanup of a single already-gone file
    renders ``0 moved · 1 skipped`` above cards that all say
    ``Cleaned up``.
    """
    _seed_resolved_scan(live_server["db"], loser_count=1)

    def handle_trash(route):
        ids = route.request.post_data_json["photo_ids"]
        route.fulfill(
            status=200,
            content_type="application/json",
            body=json.dumps({
                "ok": True,
                "trashed": 0,
                "skipped": [
                    {"id": ids[0], "reason": "file already missing"},
                ],
                "failed": [],
            }),
        )

    page.route("**/api/duplicates/delete-loser-files", handle_trash)
    page.on("dialog", lambda dialog: dialog.accept())
    page.goto(f"{live_server['url']}/duplicates")

    page.locator("#trashAllBtn").click()

    progress = page.locator("#trashAllProgress")
    expect(progress).to_be_visible()
    expect(progress).to_contain_text("Cleanup complete")
    expect(progress).to_contain_text("0 skipped")
    expect(progress).not_to_contain_text("1 skipped")
    # And the card matches — terminal completion, not skipped.
    expect(page.locator(".dup-card.trashed")).to_have_count(1)
