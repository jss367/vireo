"""Duplicate pattern summaries and actions scoped to the selected category."""

import json

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
    expect(page.locator("#summary")).to_be_hidden()
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
