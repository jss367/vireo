"""Collection coverage uses the live catalog independently of saved results."""

import json

from playwright.sync_api import expect

from e2e.test_pipeline_review_species import _write_predictionless_pipeline_cache


def test_collection_coverage_refresh_and_scope_change(live_server, page):
    db = live_server["db"]
    folder = live_server["data"]["folders"][0]
    db.conn.executemany(
        "INSERT INTO photos (folder_id, filename, extension) VALUES (?, ?, '.jpg')",
        [(folder, f"coverage-{i}.jpg") for i in range(3154)],
    )
    db.commit()
    ids = [r["id"] for r in db.conn.execute("SELECT id FROM photos")]
    assert len(ids) == 3159
    admitted = [*ids, *range(max(ids) + 1, max(ids) + 145)]
    cid = db.collections.add_import("Recorded import", admitted)
    unknown = db.collections.add("Historical import", json.dumps([
        {"field": "photo_ids", "value": ids[:1]},
    ]))
    _write_predictionless_pipeline_cache(live_server, live_server["data"]["photos"][:2])
    page.set_default_timeout(10000)
    errors = []
    page.on("pageerror", lambda error: errors.append(str(error)))
    page.goto(f"{live_server['url']}/pipeline/review")
    page.locator("#reviewScopeSelect").select_option("collection")
    page.locator("#reviewScopeCollectionSelect").select_option(str(cid))
    counts = page.locator("#collectionCoverageCounts")
    expect(counts).to_contain_text("3,303 originally imported")
    expect(counts).to_contain_text("3,159 current catalog members")
    expect(page.locator("#collectionCoverageReview")).to_contain_text("Inspected: unknown")
    page.locator("#collectionCoverage summary").click()
    expect(page.locator("#collectionCoverageDefinitions")).to_contain_text(
        "the difference does not establish deletion or data loss"
    )
    db.collections.update(cid, rules_json=json.dumps([
        {"field": "photo_ids", "value": ids[:2]},
    ]))
    page.locator("#refreshCollectionCoverage").click()
    expect(counts).to_contain_text("2 current catalog members")
    expect(counts).to_contain_text("3,303 originally imported")
    page.locator("#reviewScopeCollectionSelect").select_option(str(unknown))
    expect(counts).to_contain_text("Original import size unknown")
    expect(counts).to_contain_text("1 current catalog members")
    page.locator("#reviewScopeSelect").select_option("cache")
    expect(page.locator("#collectionCoverage")).to_be_hidden()
    assert errors == []
