"""Duplicate pattern summaries and actions scoped to the selected category."""

import json

import pytest
from playwright.sync_api import expect


def _show_patterns(live_server, page):
    specs = [
        ("numbered", "resolved", "/one/_D850589.NEF", "/one/_D850589-2.NEF"),
        ("same1", "unresolved", "/a/DSC_1127.NEF", "/b/DSC_1127.NEF"),
        ("same2", "unresolved", "/a/20160627_154208.jpg", "/b/20160627_154208.jpg"),
        ("numberedAcross", "unresolved", "/a/owl.jpg", "/b/owl-2.jpg"),
        ("renamed", "resolved", "/one/bird.jpg", "/one/renamed.jpg"),
        ("renamedAcross", "resolved", "/a/finch.jpg", "/b/export.jpg"),
        ("empty", "resolved", "/a/empty.jpg", "/b/empty.jpg"),
    ]
    proposals = []
    for index, (file_hash, status, kept, extra) in enumerate(specs):
        size = 0 if file_hash == "empty" else 1000

        def candidate(path, photo_id, size=size):
            return {"id": photo_id, "filename": path.rsplit("/", 1)[1],
                    "path": path, "file_size": size, "exists": True}

        proposals.append({
            "file_hash": file_hash, "status": status,
            "winner": candidate(kept, index * 2 + 1),
            "losers": [candidate(extra, index * 2 + 2)],
            "empty_file_group": file_hash == "empty",
        })
    result = {
        "proposals": proposals,
        "buckets": [{"folders": ["/a", "/b"], "group_count": 3,
                     "file_hashes": ["same1", "same2", "numberedAcross"],
                     "total_size": 3000, "example_filenames": ["DSC_1127.NEF", "20160627_154208.jpg", "owl.jpg"]}],
    }
    page.route("**/api/duplicates/last-scan", lambda route: route.fulfill(
        content_type="application/json",
        body=json.dumps({"found": True, "finished_at": "2026-10-12T10:00:00", "result": result}),
    ))
    page.goto(f"{live_server['url']}/duplicates")
    expect(page.locator(".category-summary")).to_be_visible()
    return proposals


def _category(page, category):
    return page.locator(f'[data-duplicate-category="{category}"]')


def test_category_summary_counts_and_filters_both_sections(live_server, page):
    _show_patterns(live_server, page)
    expect(_category(page, "all")).to_contain_text("7 groups · 7 extra copies · 5.9 KB")
    expect(_category(page, "same-name-across-folders")).to_contain_text("2 groups · 2 extra copies · 2.0 KB")
    expect(page.locator(".category-button")).to_have_count(7)

    _category(page, "numbered-same-folder").click()
    expect(_category(page, "numbered-same-folder")).to_have_attribute("aria-pressed", "true")
    expect(page.locator(".dup-group")).to_have_count(1)
    expect(page.locator(".dup-group[data-hash='numbered']")).to_be_visible()
    expect(page.locator(".category-badge")).to_have_text("Numbered copies in one folder")
    expect(page.locator("#trashAllBtn")).to_contain_text("Move 1 extra copy")
    expect(page.locator("#applyBar")).to_be_hidden()

    _category(page, "same-name-across-folders").click()
    expect(page.locator(".dup-group")).to_have_count(2)
    expect(page.locator("#applyBtn")).to_have_text("Reject 2 extra copies")
    expect(page.locator(".bucket-card")).to_contain_text("2 duplicate groups")
    expect(page.locator("#trashAllBtn")).to_have_count(0)
    _category(page, "all").click()
    expect(page.locator(".dup-group")).to_have_count(7)


def test_category_limits_bulk_folder_decisions(live_server, page):
    _show_patterns(live_server, page)
    captured = []

    def resolve(route):
        captured.append(route.request.post_data_json)
        route.fulfill(content_type="application/json", body=json.dumps({
            "resolved": [{"file_hash": "same1", "loser_ids": [4]},
                         {"file_hash": "same2", "loser_ids": [6]}],
            "skipped": [],
        }))

    page.route("**/api/duplicates/bulk-resolve", resolve)
    page.on("dialog", lambda dialog: dialog.accept())
    _category(page, "same-name-across-folders").click()
    page.locator(".bucket-card .keep-btn:not(.reveal-btn)").first.click()
    expect(page.locator(".bucket-card")).to_have_count(0)
    assert captured == [{"file_hashes": ["same1", "same2"], "keep_folder": "/a"}]
    # The category disappears after the last two groups are resolved. Reset
    # to all, retaining the hidden numbered group and the other categories.
    expect(_category(page, "all")).to_have_attribute("aria-pressed", "true")
    expect(page.locator(".dup-group[data-hash='numberedAcross']")).to_be_visible()


def test_category_limits_reject_and_trash_actions(live_server, page):
    _show_patterns(live_server, page)
    requests = []
    page.route("**/api/duplicates/apply", lambda route: (
        requests.append(route.request.post_data_json),
        route.fulfill(content_type="application/json", body='{"rejected_count": 1}'),
    ))
    page.route("**/api/duplicates/delete-loser-files", lambda route: (
        requests.append(route.request.post_data_json),
        route.fulfill(content_type="application/json", body='{"trashed": 1, "skipped": [], "failed": []}'),
    ))
    page.on("dialog", lambda dialog: dialog.accept())
    _category(page, "numbered-same-folder").click()
    page.locator("#trashAllBtn").click()
    expect(page.locator("#trashAllProgress")).to_contain_text("Cleanup complete")
    assert requests == [{"photo_ids": [2]}]
    expect(_category(page, "numbered-same-folder")).to_contain_text("0 groups · 0 extra copies")
    expect(page.locator(".summary-bar")).not_to_contain_text("Cleanup pending")
    _category(page, "all").click()
    expect(page.locator(".dup-group[data-hash='numbered']")).to_have_count(0)
    expect(_category(page, "all")).to_contain_text("6 groups · 6 extra copies")
    _category(page, "numbered-across-folders").click()
    page.locator("#applyBtn").click()
    expect(page.locator(".dup-group[data-hash='numberedAcross']")).to_have_count(0)
    expect(_category(page, "all")).to_contain_text("5 groups · 5 extra copies")
    expect(page.locator(".dup-group[data-hash='same1']")).to_be_visible()
    expect(page.locator("#applyStatus")).to_have_text("")
    assert requests == [{"photo_ids": [2]}, {"hashes": ["numberedAcross"]}]


def test_category_counts_respect_library_filters_and_protected_groups(live_server, page):
    _show_patterns(live_server, page)
    page.evaluate("""() => {
      _filterMatchedIds = new Set([1, 3]);
      _proposals[0].winner.exists = false;
      renderResults(_lastScanResult);
    }""")
    expect(_category(page, "all")).to_contain_text("2 groups · 2 extra copies")
    expect(page.locator(".category-button")).to_have_count(3)
    _category(page, "numbered-same-folder").click()
    expect(page.locator(".dup-group")).to_have_count(1)
    expect(page.locator("#trashAllBtn")).to_have_count(0)
    expect(page.locator(".trash-btn")).to_have_count(0)
    expect(page.locator(".dup-group .group-warning")).to_contain_text("kept file")


def test_category_switch_preserves_exclusions_and_remaining_results(live_server, page):
    _show_patterns(live_server, page)
    requests = []
    page.route("**/api/duplicates/apply", lambda route: (
        requests.append(route.request.post_data_json),
        route.fulfill(content_type="application/json", body='{"rejected_count": 1}'),
    ))
    _category(page, "same-name-across-folders").click()
    excluded = page.locator(".dup-group[data-hash='same1'] .group-check")
    excluded.uncheck()
    _category(page, "numbered-across-folders").click()
    _category(page, "same-name-across-folders").click()
    expect(excluded).not_to_be_checked()
    expect(page.locator("#applyBtn")).to_have_text("Reject 1 extra copy")
    page.locator("#applyBtn").click()
    expect(page.locator(".dup-group[data-hash='same2']")).to_have_count(0)
    expect(excluded).not_to_be_checked()
    expect(page.locator("#applyBtn")).to_be_disabled()
    assert requests == [{"hashes": ["same2"]}]
    expect(_category(page, "all")).to_contain_text("6 groups · 6 extra copies")
    _category(page, "all").click()
    expect(page.locator(".dup-group[data-hash='numberedAcross']")).to_be_visible()
    expect(excluded).not_to_be_checked()


def test_scoped_rejection_preserves_deferred_and_unsubmitted_groups(live_server, page):
    _show_patterns(live_server, page)
    page.route("**/api/duplicates/apply", lambda route: route.fulfill(
        content_type="application/json",
        body='{"rejected_count": 1, "deferred_hashes": ["same2"]}',
    ))
    _category(page, "same-name-across-folders").click()
    page.locator("#applyBtn").click()
    expect(page.locator(".dup-group[data-hash='same1']")).to_have_count(0)
    expect(page.locator(".dup-group[data-hash='same2'] .group-check")).to_be_checked()
    expect(page.locator("#applyStatus")).to_contain_text("1 group deferred")
    expect(_category(page, "all")).to_contain_text("6 groups · 6 extra copies")
    _category(page, "numbered-across-folders").click()
    expect(page.locator(".dup-group[data-hash='numberedAcross']")).to_be_visible()


def test_partial_cleanup_moves_remaining_actions_to_updated_category(live_server, page):
    _show_patterns(live_server, page)
    page.evaluate("""() => {
      var group = _proposals.find(p => p.file_hash === 'same1');
      group.status = 'resolved';
      group.losers.push({id: 20, filename: 'DSC_1127-2.NEF',
        path: '/a/DSC_1127-2.NEF', file_size: 1000, exists: true});
      renderResults(_lastScanResult);
    }""")
    requests = []
    page.route("**/api/duplicates/delete-loser-files", lambda route: (
        requests.append(route.request.post_data_json),
        route.fulfill(content_type="application/json", body='{"trashed": 1, "skipped": [], "failed": []}'),
    ))
    _category(page, "numbered-across-folders").click()
    page.locator(".dup-card[data-photo-id='20'] .trash-btn").click()
    expect(page.locator(".cleanup-category-note")).to_contain_text("1 extra copy cleaned up")
    expect(page.locator(".cleanup-category-note")).to_contain_text("Same name across folders")
    expect(page.locator(".dup-group[data-hash='same1']")).to_have_count(0)
    expect(page.locator("#trashAllBtn")).to_have_count(0)
    expect(page.locator(".trash-btn")).to_have_count(0)
    assert requests == [{"photo_ids": [20]}]
    page.locator(".cleanup-category-note button").click()
    expect(page.locator(".dup-group[data-hash='same1'] .category-badge")).to_have_text("Same name across folders")
    expect(page.locator(".dup-card[data-photo-id='4'] .trash-btn")).to_be_visible()
    expect(page.locator("#trashAllBtn")).to_contain_text("Move 1 extra copy")


def test_inflight_cleanup_renders_group_entering_selected_category(live_server, page):
    _show_patterns(live_server, page)
    page.evaluate("""() => {
      var group = _proposals.find(p => p.file_hash === 'same1');
      group.status = 'resolved';
      group.losers.push({id: 20, filename: 'DSC_1127-2.NEF',
        path: '/a/DSC_1127-2.NEF', file_size: 1000, exists: true});
      renderResults(_lastScanResult);
    }""")
    pending = []
    page.route("**/api/duplicates/delete-loser-files", lambda route: pending.append(route))
    _category(page, "numbered-across-folders").click()
    with page.expect_request("**/api/duplicates/delete-loser-files"):
        page.locator(".dup-card[data-photo-id='20'] .trash-btn").click()
    _category(page, "same-name-across-folders").click()
    expect(page.locator(".dup-group[data-hash='same1']")).to_have_count(0)
    pending[0].fulfill(content_type="application/json", body='{"trashed": 1, "skipped": [], "failed": []}')
    expect(page.locator(".dup-group[data-hash='same1'] .category-badge")).to_have_text("Same name across folders")
    expect(page.locator(".dup-card[data-photo-id='4'] .trash-btn")).to_be_visible()
    expect(page.locator("#trashAllBtn")).to_contain_text("Move 1 extra copy")
    expect(page.locator(".dup-card[data-photo-id='20']")).to_have_count(0)


def test_inflight_batch_cleanup_keeps_progress_and_retry_after_category_switch(live_server, page):
    _show_patterns(live_server, page)
    page.evaluate("""() => {
      var group = _proposals.find(p => p.file_hash === 'same1');
      group.status = 'resolved';
      group.losers.push({id: 20, filename: 'DSC_1127-2.NEF',
        path: '/a/DSC_1127-2.NEF', file_size: 1000, exists: true});
      renderResults(_lastScanResult);
    }""")
    pending = []
    page.route("**/api/duplicates/delete-loser-files", lambda route: pending.append(route))
    page.on("dialog", lambda dialog: dialog.accept())
    _category(page, "numbered-across-folders").click()
    with page.expect_request("**/api/duplicates/delete-loser-files"):
        page.locator("#trashAllBtn").click()
    expect(page.locator("#trashAllProgress")).to_contain_text("Moving files")
    _category(page, "same-name-across-folders").click()
    expect(page.locator("#trashAllProgress")).to_be_visible()
    pending[0].fulfill(content_type="application/json", body=json.dumps({
        "trashed": 1, "skipped": [], "failed": [{"id": 4, "error": "temporarily unavailable"}],
    }))
    expect(page.locator(".dup-group[data-hash='same1']")).to_be_visible()
    expect(page.locator(".dup-card[data-photo-id='4'] [data-trash-status]")).to_contain_text("temporarily unavailable")
    expect(page.locator("#trashAllProgress")).to_contain_text("Cleanup finished with problems")
    expect(page.locator("#trashAllProgress")).to_contain_text("1 moved")
    expect(page.locator("#trashAllProgress")).to_contain_text("1 failed")
    expect(page.locator("#trashAllBtn")).to_have_text("Move 1 extra copy to Trash")
    expect(page.locator("#trashAllBtn")).to_be_enabled()
    with page.expect_request("**/api/duplicates/delete-loser-files"):
        page.locator("#trashAllBtn").click()
    assert [route.request.post_data_json for route in pending] == [{"photo_ids": [4, 20]}, {"photo_ids": [4]}]
    pending[1].fulfill(content_type="application/json", body='{"trashed": 1, "skipped": [], "failed": []}')
    expect(page.locator("#trashAllProgress")).to_contain_text("Cleanup complete")
    expect(page.locator("#trashAllBtn")).to_have_count(0)


def test_pending_cleanup_reconciles_newly_matching_cards_and_bulk_retry(live_server, page):
    _show_patterns(live_server, page)
    page.evaluate("""() => {
      function photo(id, path) {
        return {id, path, filename: path.split('/').pop(), file_size: 1000, exists: true};
      }
      renderResults({proposals: [
        {file_hash: 'mixed', status: 'resolved', winner: photo(1, '/a/owl.jpg'),
          losers: [photo(2, '/a/owl-2.jpg'), photo(3, '/b/owl-3.jpg')]},
        {file_hash: 'other', status: 'resolved', winner: photo(11, '/c/hawk.jpg'),
          losers: [photo(12, '/c/hawk-2.jpg')]},
        {file_hash: 'excluded', status: 'unresolved', winner: photo(21, '/a/finch.jpg'),
          losers: [photo(22, '/b/finch.jpg')]}
      ], buckets: []});
    }""")
    _category(page, "same-name-across-folders").click()
    page.locator(".group-check").uncheck()
    requests, pending, confirmations = [], [], []

    def trash(route):
        requests.append(route.request.post_data_json)
        if len(requests) == 1:
            pending.append(route)
        else:
            response = {"trashed": 1, "skipped": [], "failed": []}
            if len(requests) == 2:
                response["failed"] = [{"id": 2, "error": "Synthetic locked file"}]
            route.fulfill(content_type="application/json", body=json.dumps(response))

    page.route("**/api/duplicates/delete-loser-files", trash)
    page.on("dialog", lambda dialog: (confirmations.append(dialog.message), dialog.accept()))
    _category(page, "numbered-across-folders").click()
    page.locator(".dup-card[data-photo-id='3'] .trash-btn").click()
    assert requests == [{"photo_ids": [3]}]
    _category(page, "numbered-same-folder").click()
    expect(page.locator(".dup-group[data-hash='mixed']")).to_have_count(0)
    expect(page.locator("#trashAllBtn")).to_contain_text("Move 1 extra copy")
    pending[0].fulfill(content_type="application/json", body=json.dumps(
        {"trashed": 1, "skipped": [], "failed": []},
    ))
    expect(page.locator(".dup-group[data-hash='mixed']")).to_have_count(1)
    expect(page.locator(".dup-card[data-photo-id='3']")).to_have_count(0)
    expect(page.locator("#trashAllBtn")).to_contain_text("Move 2 extra copies")
    assert page.locator(".dup-card.loser").evaluate_all(
        "cards => cards.map(c => Number(c.dataset.photoId))"
    ) == [2, 12]
    page.locator("#trashAllBtn").click()
    expect(page.locator(".dup-card[data-photo-id='2'] [data-trash-status]")).to_contain_text(
        "Failed: Synthetic locked file",
    )
    expect(page.locator("#trashAllBtn")).to_contain_text("Move 1 extra copy")
    page.locator("#trashAllBtn").click()
    expect(page.locator("#trashAllBtn")).to_have_count(0)
    assert requests == [{"photo_ids": [3]}, {"photo_ids": [2, 12]}, {"photo_ids": [2]}]
    assert confirmations[0].startswith("Move 2 files to Trash")
    assert confirmations[1].startswith("Move 1 file to Trash")
    _category(page, "all").click()
    expect(page.locator(".dup-group[data-hash='mixed']")).to_have_count(0)
    expect(page.locator(".dup-group[data-hash='other']")).to_have_count(0)
    expect(page.locator(".dup-group[data-hash='excluded'] .group-check")).not_to_be_checked()


def test_category_switch_preserves_bulk_folder_trash_retry(live_server, page):
    _show_patterns(live_server, page)
    resolve_requests, pending = [], []

    def resolve(route):
        resolve_requests.append(route.request.post_data_json)
        route.fulfill(content_type="application/json", body=json.dumps({
            "resolved": [{"file_hash": "same1", "loser_ids": [4]},
                         {"file_hash": "same2", "loser_ids": [6]}], "skipped": [],
        }))

    page.route("**/api/duplicates/bulk-resolve", resolve)
    page.route("**/api/duplicates/delete-loser-files", lambda route: pending.append(route))
    page.on("dialog", lambda dialog: dialog.accept())
    _category(page, "same-name-across-folders").click()
    page.locator(".bucket-card input[data-delete-toggle]").check()
    with page.expect_request("**/api/duplicates/delete-loser-files"):
        page.locator(".bucket-card .keep-btn:not(.reveal-btn)").first.click()
    _category(page, "numbered-across-folders").click()
    pending[0].fulfill(content_type="application/json", body=json.dumps({
        "trashed": 1, "skipped": [], "failed": [{"id": 6, "error": "temporarily unavailable"}],
    }))
    retry = page.locator(".bucket-card [data-retry-trash]")
    expect(retry).to_be_visible()
    expect(retry).to_have_text("Retry 1 file to Trash")
    _category(page, "all").click()
    expect(retry).to_be_visible()
    expect(page.locator(".dup-group[data-hash='same1']")).to_have_count(0)
    expect(page.locator(".dup-group[data-hash='same2']")).to_have_count(0)
    with page.expect_request("**/api/duplicates/delete-loser-files"):
        retry.click()
    assert [route.request.post_data_json for route in pending] == [{"photo_ids": [4, 6]}, {"photo_ids": [6]}]
    assert resolve_requests == [{"file_hashes": ["same1", "same2"], "keep_folder": "/a"}]
    pending[1].fulfill(content_type="application/json", body='{"trashed": 1, "skipped": [], "failed": []}')
    expect(page.locator(".bucket-card")).to_have_count(0)
    expect(page.locator(".dup-group[data-hash='numberedAcross']")).to_be_visible()


@pytest.mark.parametrize("outcome", ["failed", "skipped", "network"])
def test_single_file_trash_state_survives_category_renders(live_server, page, outcome):
    _show_patterns(live_server, page)
    pending = []
    page.route("**/api/duplicates/delete-loser-files", lambda route: pending.append(route))
    _category(page, "numbered-same-folder").click()
    button = page.locator(".dup-card[data-photo-id='2'] .trash-btn")
    status = page.locator(".dup-card[data-photo-id='2'] [data-trash-status]")
    with page.expect_request("**/api/duplicates/delete-loser-files"):
        button.click()
    _category(page, "all").click()
    expect(page.locator("#trashAllBtn")).to_have_text("Move 3 extra copies to Trash")
    _category(page, "numbered-same-folder").click()
    expect(button).to_be_disabled()
    expect(status).to_contain_text("Moving to Trash")
    page.evaluate("trashOneLoserFile(2)")
    assert len(pending) == 1
    if outcome == "network":
        pending[0].abort("connectionfailed")
    else:
        payload = {"trashed": 0, "skipped": [], "failed": []}
        payload[outcome] = [{"id": 2, "reason": "loser volume offline"} if outcome == "skipped"
                            else {"id": 2, "error": "locked file"}]
        pending[0].fulfill(content_type="application/json", body=json.dumps(payload))
    message = "Skipped: loser volume offline" if outcome == "skipped" else "Failed"
    expect(status).to_contain_text(message)
    expect(button).to_be_enabled()
    _category(page, "same-name-across-folders").click()
    _category(page, "numbered-same-folder").click()
    expect(status).to_contain_text(message)
    expect(button).to_be_enabled()
    with page.expect_request("**/api/duplicates/delete-loser-files"):
        button.click()
    assert [route.request.post_data_json for route in pending] == [{"photo_ids": [2]}, {"photo_ids": [2]}]
    pending[1].fulfill(content_type="application/json", body='{"trashed": 1, "skipped": [], "failed": []}')
    expect(page.locator(".dup-card[data-photo-id='2']")).to_have_class("dup-card loser trashed")
    expect(status).to_have_text("Moved to Trash")
    expect(button).to_be_disabled()


def test_bulk_trash_owns_per_file_controls_across_category_renders(live_server, page):
    _show_patterns(live_server, page)
    pending = []
    page.route("**/api/duplicates/delete-loser-files", lambda route: pending.append(route))
    page.on("dialog", lambda dialog: dialog.accept())
    _category(page, "numbered-same-folder").click()
    with page.expect_request("**/api/duplicates/delete-loser-files"):
        page.locator("#trashAllBtn").click()
    button = page.locator(".dup-card[data-photo-id='2'] .trash-btn")
    expect(button).to_be_disabled()
    _category(page, "all").click()
    _category(page, "numbered-same-folder").click()
    expect(button).to_be_disabled()
    page.evaluate("trashOneLoserFile(2)")
    assert len(pending) == 1
    pending[0].fulfill(content_type="application/json", body=json.dumps({
        "trashed": 0, "skipped": [], "failed": [{"id": 2, "error": "batch file is locked"}],
    }))
    expect(button).to_be_enabled()
    expect(page.locator("#trashAllBtn")).to_have_text("Move 1 extra copy to Trash")
    _category(page, "all").click()
    _category(page, "numbered-same-folder").click()
    expect(page.locator(".dup-card[data-photo-id='2'] [data-trash-status]")).to_have_text("Failed: batch file is locked")


@pytest.mark.parametrize("outcome", ["failed", "skipped", "network"])
def test_bulk_outcome_replaces_previous_single_file_failure(live_server, page, outcome):
    _show_patterns(live_server, page)
    pending = []
    page.route("**/api/duplicates/delete-loser-files", lambda route: pending.append(route))
    page.on("dialog", lambda dialog: dialog.accept())
    _category(page, "numbered-same-folder").click()
    with page.expect_request("**/api/duplicates/delete-loser-files"):
        page.locator(".dup-card[data-photo-id='2'] .trash-btn").click()
    pending[0].fulfill(content_type="application/json", body=json.dumps({
        "trashed": 0, "skipped": [], "failed": [{"id": 2, "error": "old single-file error"}],
    }))
    status = page.locator(".dup-card[data-photo-id='2'] [data-trash-status]")
    expect(status).to_have_text("Failed: old single-file error")
    with page.expect_request("**/api/duplicates/delete-loser-files"):
        page.locator("#trashAllBtn").click()
    if outcome == "network":
        pending[1].abort("connectionfailed")
        message = "Failed"
    else:
        payload = {"trashed": 0, "skipped": [], "failed": []}
        payload[outcome] = [{"id": 2, "reason": "new offline reason"} if outcome == "skipped"
                            else {"id": 2, "error": "new batch error"}]
        pending[1].fulfill(content_type="application/json", body=json.dumps(payload))
        message = "Skipped: new offline reason" if outcome == "skipped" else "Failed: new batch error"
    expect(status).not_to_contain_text("old single-file error")
    expect(page.locator("#trashAllBtn")).to_be_enabled()
    _category(page, "same-name-across-folders").click()
    _category(page, "numbered-same-folder").click()
    expect(status).to_contain_text(message)
    expect(status).not_to_contain_text("old single-file error")



def test_finishing_batch_does_not_release_a_newer_single_file_retry(live_server, page):
    _show_patterns(live_server, page)
    page.evaluate("""() => {
      function photo(id, name) {
        return {id, filename: name, path: '/a/' + name, file_size: 1000, exists: true};
      }
      var proposals = [];
      for (var i = 0; i < 26; i++) {
        proposals.push({file_hash: 'batch' + i, status: 'resolved',
          winner: photo(100 + i * 2, 'bird-' + i + '.jpg'),
          losers: [photo(101 + i * 2, 'bird-' + i + '-2.jpg')]});
      }
      renderResults({proposals, buckets: []});
    }""")
    pending = []
    page.route("**/api/duplicates/delete-loser-files", lambda route: pending.append(route))
    page.on("dialog", lambda dialog: dialog.accept())
    _category(page, "numbered-same-folder").click()
    with page.expect_request("**/api/duplicates/delete-loser-files"):
        page.locator("#trashAllBtn").click()
    with page.expect_request("**/api/duplicates/delete-loser-files"):
        pending[0].fulfill(content_type="application/json", body=json.dumps({
            "trashed": 24, "skipped": [], "failed": [{"id": 101, "error": "retry first file"}],
        }))
    button = page.locator(".dup-card[data-photo-id='101'] .trash-btn")
    expect(button).to_be_enabled()
    with page.expect_request("**/api/duplicates/delete-loser-files"):
        button.click()
    pending[1].fulfill(content_type="application/json", body='{"trashed": 1, "skipped": [], "failed": []}')
    expect(page.locator("#trashAllProgress")).to_contain_text("Cleanup finished with problems")
    expect(button).to_be_disabled()
    expect(page.locator(".dup-card[data-photo-id='101'] [data-trash-status]")).to_contain_text("Moving to Trash")
    page.evaluate("trashOneLoserFile(101)")
    assert len(pending) == 3
    pending[2].fulfill(content_type="application/json", body='{"trashed": 1, "skipped": [], "failed": []}')
    expect(page.locator(".dup-card[data-photo-id='101']")).to_have_class("dup-card loser trashed")
