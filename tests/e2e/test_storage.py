import os
from pathlib import Path

import pytest
from playwright.sync_api import expect

from e2e.module_pages import install_page_module_exposure


@pytest.fixture
def page(browser, page):
    """The Storage page's modules as ``pageModules.<file>`` and its state as
    ``storageState`` after every navigation (see module_pages.py). Asks for
    ``browser`` first, as conftest's ``page`` does, so the browser launches
    before ``live_server`` moves HOME."""
    return install_page_module_exposure(page, "storage", "storageState")


def test_storage_shows_named_categories_and_escaped_file_paths(live_server, page):
    db_path = live_server["db"]._db_path
    # Literal angle brackets are invalid in Windows filenames. Encoded markup
    # still catches missing HTML escaping: its displayed path must stay exact.
    backup = Path(f"{db_path}.bak-&lt;img src=x onerror=alert(1)&gt;")
    backup.write_bytes(b"backup" * 1024)
    originals = Path(live_server["app"].config["THUMB_CACHE_DIR"]).parent / "originals"
    originals.mkdir(exist_ok=True)
    (originals / "1.display.jpg").write_bytes(b"render" * 1024)

    page.goto(f"{live_server['url']}/storage")
    expect(page.locator("#storageGrid")).to_contain_text("Catalog backups")
    expect(page.locator("#storageGrid")).to_contain_text("Full-resolution renders")
    expect(page.locator("#storageGrid")).not_to_contain_text("Other Vireo Data")
    details = page.locator("#storageBreakdown details").filter(has_text="Catalog backups")
    details.locator("summary").click()
    expect(details.locator("code").filter(has_text=backup.name)).to_be_visible()
    expect(details.locator(".storage-breakdown-row").filter(has_text=backup.name)).to_contain_text("6 KB")
    expect(details.locator("img")).to_have_count(0)


def test_quota_apply_aborts_when_pending_storage_autosave_fails(
    live_server, page,
):
    """A failed queued autosave must prevent the quota POST and stay editable."""
    posted_payloads = []

    def config_route(route, request):
        if request.method != "POST":
            route.continue_()
            return
        payload = request.post_data_json
        posted_payloads.append(payload)
        if "working_copy_cache_max_mb" in payload:
            route.fulfill(status=200, json={"ok": True})
        else:
            route.fulfill(status=500, json={"error": "autosave failed"})

    page.route("**/api/config", config_route)
    page.goto(f"{live_server['url']}/storage", timeout=5000)

    page.evaluate(
        """async () => {
          document.getElementById('cfgPreviewCacheMaxMb').value = '123';
          pageModules.config.saveStorageConfig();
          await pageModules.workingCopyLimit.commitWorkingCopyLimit(40960, { confirmed: false });
        }"""
    )

    assert posted_payloads
    assert not any(
        "working_copy_cache_max_mb" in payload for payload in posted_payloads
    )
    expect(page.locator("#cfgWorkingCopyCacheMaxGb")).to_be_enabled()
    expect(page.locator("body")).to_contain_text(
        "the working-copy limit was not updated"
    )


def test_retained_storage_lists_files_and_confirms_cleanup_in_page(live_server, page):
    root = Path(live_server["app"].config["THUMB_CACHE_DIR"]).parent
    render = root / "originals" / "1.display.jpg"
    render.parent.mkdir(exist_ok=True)
    render.write_bytes(b"render" * 1024)
    page.goto(f"{live_server['url']}/storage")
    details = page.locator("#retainedStorageItems details").filter(has_text="Full-resolution renders")
    expect(details).to_contain_text("6 KB")
    details.locator("summary").click()
    expect(details).to_contain_text("1.display.jpg")
    details.get_by_role("button", name="Clear full-resolution renders").click()
    expect(page.locator("#retainedActionModal")).to_be_visible()
    page.locator("#retainedActionCancel").click()
    assert render.exists()
    details.get_by_role("button", name="Clear full-resolution renders").click()
    page.locator("#retainedActionConfirm").click()
    expect(page.locator("#retainedStorageStatus")).to_contain_text("completed")
    assert not render.exists()
    expect(page.locator("#retainedStorageItems")).to_contain_text("Grouping undo snapshots")
    if screenshot := os.environ.get("VIREO_STORAGE_SCREENSHOT"):
        page.locator("#retainedCacheLimit").scroll_into_view_if_needed()
        page.screenshot(path=screenshot)


def test_storage_retention_limits_apply_to_global_settings(live_server, page):
    page.goto(f"{live_server['url']}/storage")
    expect(page.locator("#retainedCacheLimit")).to_have_value("10240")
    page.locator("#retainedCacheLimit").fill("2048")
    page.locator("#retainedCacheAge").fill("45")
    page.locator("#groupingHistoryLimit").fill("64")
    page.get_by_role("button", name="Apply retention limits").click()
    expect(page.locator("#retainedActionDescription")).to_contain_text("every workspace")
    page.locator("#retainedActionConfirm").click()
    expect(page.locator("#retainedStorageStatus")).to_contain_text("completed")
    result = page.evaluate("safeFetch('/api/storage/retained')")
    assert result["policy"] == {"retained_cache_max_mb": 2048, "retained_cache_max_age_days": 45, "grouping_history_max_mb": 64}


def test_workspace_delete_explains_shared_analysis_and_removes_saved_results(live_server, page):
    db = live_server["db"]
    workspace = db.workspaces.id_for_name('Field Work')
    result = Path(db._db_path).parent / f"culling_results_ws{workspace}.json"
    result.write_text('{}')
    page.goto(f"{live_server['url']}/workspace")
    row = page.locator('#workspacesContent .setting-row').filter(
        has=page.locator('input[value="Field Work"]'),
    )
    expect(row).to_have_count(1)
    row.get_by_role('button', name='Delete', exact=True).click()
    expect(page.locator('#workspaceDeleteDialog')).to_contain_text('Shared photos, keywords, and photo analysis remain')
    page.locator('#workspaceDeleteConfirm').click()
    expect(page.locator('#workspaceDeleteDialog')).to_have_count(0)
    expect(row).to_have_count(0)
    assert not result.exists()


def test_storage_sort_preserves_load_errors_and_recovers(live_server, page):
    fail_storage = True

    def storage_route(route):
        if fail_storage:
            route.fulfill(status=503, json={"error": "Storage unavailable"})
        else:
            route.continue_()

    page.route("**/api/storage", storage_route)
    page.goto(f"{live_server['url']}/storage")
    grid = page.locator("#storageGrid")
    sort = page.get_by_label("Sort by", exact=True)
    expect(grid).to_have_text("Failed to load storage info")
    expect(sort).to_be_disabled()
    page.evaluate("pageModules.overview.renderStorageCards()")
    expect(grid).to_have_text("Failed to load storage info")

    fail_storage = False
    page.evaluate("pageModules.panel.loadStoragePanel()")
    expect(sort).to_be_enabled()
    expect(grid.locator(".stat-card").first).to_be_visible()
    sort.select_option("name")

    fail_storage = True
    page.evaluate("pageModules.panel.loadStoragePanel()")
    expect(sort).to_be_disabled()
    page.evaluate("pageModules.overview.renderStorageCards()")
    expect(grid).to_have_text("Failed to load storage info")
    expect(grid.locator(".stat-card")).to_have_count(0)

    fail_storage = False
    page.evaluate("pageModules.panel.loadStoragePanel()")
    expect(sort).to_be_enabled()
    expect(sort).to_have_value("name")
    expect(grid.locator(".stat-card").first).to_be_visible()
