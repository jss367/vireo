"""Same-shot candidates remain visible, filterable and strictly review-only."""
import json
import os
from pathlib import Path

import pytest
from duplicate_scan import run_duplicate_scan
from PIL import Image, ImageDraw
from playwright.sync_api import expect
from services import photo_ingest, workspace_folders

from e2e.module_pages import install_page_module_exposure


@pytest.fixture
def page(browser, page):
    """The Duplicates page's modules as ``pageModules.<file>`` and its state as
    ``duplicatesState`` after every navigation (see module_pages.py). Asks for
    ``browser`` first, as conftest's ``page`` does, so the browser launches
    before ``live_server`` moves HOME."""
    return install_page_module_exposure(page, "duplicates", "duplicatesState")


def seed_same_shot_review(db, tmp_path, count=2):
    active = db.active_workspace_id
    photos = []
    for index, date in enumerate(("2023-01-01T01:02:03", "2026-04-03T01:02:03", "2026-04-03T01:02:04")[:count]):
        if index:
            db.set_active_workspace(db.create_workspace("Corrected dates" if index == 1 else "Other version"))
        folder = tmp_path / ("Original camera date", "Corrected capture date", "Other version")[index]
        folder.mkdir()
        path = folder / "DSC_0386.jpg"
        image = Image.new("RGB", (480, 320), "#657a62")
        draw = ImageDraw.Draw(image)
        draw.ellipse((180, 95, 330, 220), fill="#c5a078")
        draw.ellipse((280, 70, 350, 130), fill="#c5a078")
        draw.polygon(((345, 94), (375, 105), (345, 110)), fill="#39352c")
        draw.ellipse((328, 87, 336, 95), fill="#222222")
        image.save(path)
        fid = workspace_folders.add_folder(db, str(folder))
        pid = photo_ingest.add_photo(db, folder_id=fid, filename=path.name, extension=".jpg",
                                    file_size=path.stat().st_size, file_mtime=path.stat().st_mtime,
                                    file_hash=f"version-{index}", timestamp=date)
        metadata = {"EXIF": {"Make": "Nikon", "Model": "Z 6", "SerialNumber": "SN-12345"},
                    "MakerNotes": {"ShutterCount": 386}}
        db.conn.execute("UPDATE photos SET exif_data=?, phash='123456789abcdef0' WHERE id=?",
                        (json.dumps(metadata), pid))
        photos.append(pid)
    db.set_active_workspace(active)
    db.commit()
    result = run_duplicate_scan({}, db)
    db.conn.execute(
        "INSERT INTO job_history (id,type,status,started_at,finished_at,result) "
        "VALUES ('same-shot-e2e','duplicate-scan','completed','2026-10-24T10:00:00',"
        "'2026-10-24T10:00:01',?)", (json.dumps(result),),
    )
    db.commit()
    return photos


def test_same_shot_review_has_no_cleanup_actions_and_respects_filters(live_server, page, tmp_path):
    page.set_viewport_size({"width": 1280, "height": 1080})
    a, b = seed_same_shot_review(live_server["db"], tmp_path)
    requests = []
    page.on("request", lambda request: requests.append(request.url))
    page.goto(f"{live_server['url']}/duplicates")
    section = page.locator("#sameShotResults")
    expect(section.locator(".same-shot-group")).to_have_count(1)
    expect(section.locator(".badge")).to_have_text(["REVIEW", "REVIEW"])
    expect(section).to_contain_text("Stored visual hashes also match")
    expect(section).to_contain_text("2023-01-01 01:02:03")
    expect(section).to_contain_text("2026-04-03 01:02:03")
    expect(section).to_contain_text("Corrected dates")
    expect(section.locator("input, .trash-btn")).to_have_count(0)
    expect(page.locator("#applyBar")).to_be_hidden()
    for image in section.locator("img").all():
        expect(image).to_have_js_property("complete", True)
        expect(image).not_to_have_js_property("naturalWidth", 0)
    if screenshot := os.environ.get("VIREO_REVIEW_SCREENSHOT"):
        Path(screenshot).parent.mkdir(parents=True, exist_ok=True)
        page.screenshot(path=screenshot, full_page=True)
    # Matching one member must show the complete group, including other workspaces.
    page.evaluate("id => { duplicatesState.filterMatchedIds = new Set([id]); pageModules.results.renderResults(duplicatesState.lastScanResult); }", a)
    expect(section.locator(".dup-card")).to_have_count(2)
    page.evaluate("duplicatesState.filterMatchedIds = new Set(); pageModules.results.renderResults(duplicatesState.lastScanResult)")
    expect(section.locator(".same-shot-group")).to_have_count(0)
    expect(section).to_contain_text("No possible same-shot groups match the active photo filters")
    page.evaluate("duplicatesState.filterMatchedIds = null; pageModules.results.renderResults(duplicatesState.lastScanResult)")
    page.route("**/api/files/reveal", lambda route: route.fulfill(
        json={"ok": True, "path": "example.jpg"}))
    with page.expect_request("**/api/files/reveal") as reveal:
        section.locator(f'[data-same-shot-reveal="{b}"]').click()
    assert reveal.value.post_data_json == {"photo_id": b, "scope": "duplicates"}
    assert not any(url.endswith(("/duplicates/apply", "/duplicates/keep", "/duplicates/delete-loser-files"))
                   for url in requests)
    # A large candidate set should render in batches rather than building
    # thousands of cards before the user can interact with the page.
    page.evaluate("""() => {
      const result = {...duplicatesState.lastScanResult};
      result.same_shot_groups = Array(51).fill(result.same_shot_groups[0]);
      pageModules.results.renderResults(result);
    }""")
    expect(section.locator(".same-shot-group")).to_have_count(50)
    expect(section).to_contain_text("Showing 50 of 51 likely groups")
    section.locator("[data-same-shot-more]").click()
    expect(section.locator(".same-shot-group")).to_have_count(51)


def _add_photo(db, folder, name, file_hash, *, timestamp, exif=None, phash=None):
    folder.mkdir(exist_ok=True)
    fid = workspace_folders.add_folder(db, str(folder))
    pid = photo_ingest.add_photo(db, folder_id=fid, filename=name, extension=".jpg", file_size=4000,
                                file_mtime=100.0, file_hash=file_hash, timestamp=timestamp)
    db.conn.execute("UPDATE photos SET exif_data=?, phash=? WHERE id=?",
                    (json.dumps(exif) if exif else None, phash, pid))
    return pid


def test_top_stat_leads_to_likely_copies_and_reused_counts_stay_collapsed(live_server, page, tmp_path):
    """Exact-copy results fill the top of the page; the same-shot list sits below them."""
    db = live_server["db"]
    seed_same_shot_review(db, tmp_path)
    _add_photo(db, tmp_path / "exact-a", "copy.jpg", "exact", timestamp="2024-05-01T10:00:00")
    _add_photo(db, tmp_path / "exact-b", "copy.jpg", "exact", timestamp="2024-05-01T10:00:00")
    reused = {"EXIF": {"Make": "Nikon", "Model": "D850", "SerialNumber": "3066996"},
              "MakerNotes": {"ShutterCount": 27930}}
    _add_photo(db, tmp_path / "Costa Rica", "D85_7699.jpg", "trip", timestamp="2020-01-08T05:57:43",
               exif=reused, phash="91c7797c061f6538")
    _add_photo(db, tmp_path / "USA", "_D850037.jpg", "home", timestamp="2020-03-07T17:19:11",
               exif=reused, phash="84817b7d240b7b75")
    db.commit()
    result = run_duplicate_scan({}, db)
    db.conn.execute("UPDATE job_history SET result=? WHERE id='same-shot-e2e'", (json.dumps(result),))
    db.commit()

    page.set_viewport_size({"width": 1280, "height": 900})
    page.goto(f"{live_server['url']}/duplicates")
    stat = page.locator("#summary a.summary-stat-link")
    expect(stat).to_contain_text("1")
    expect(stat).to_contain_text("Same-shot to review")
    stat.click()
    section = page.locator("#sameShotResults")
    expect(section).to_be_in_viewport()
    expect(section.locator(".same-shot-group")).to_have_count(1)
    expect(section).to_contain_text("Showing 1 of 1 likely group")
    expect(section).to_contain_text("1 more group with a matching shutter count but different images and capture times")
    expect(section).not_to_contain_text("Shutter count 27930")
    section.get_by_role("button", name="Show these groups").click()
    expect(section.locator(".same-shot-group")).to_have_count(2)
    expect(section.locator(".same-shot-group").last).to_contain_text("Shutter count 27930")

    section.locator(".same-shot-group").first.get_by_role("button", name="Keep all copies").click()
    expect(stat.locator(".num")).to_have_text("0")


def test_scan_discovers_camera_matches_without_an_exact_duplicate(live_server, page, tmp_path):
    seed_same_shot_review(live_server["db"], tmp_path)
    live_server["db"].conn.execute("DELETE FROM job_history WHERE id='same-shot-e2e'")
    live_server["db"].commit()
    page.goto(f"{live_server['url']}/duplicates")
    page.locator("#scanBtn").click()
    expect(page.locator("#sameShotResults .same-shot-group")).to_have_count(1)
    expect(page.locator("#applyBar")).to_be_hidden()
    page.reload()
    expect(page.locator("#sameShotResults .same-shot-group")).to_have_count(1)


def test_same_shot_stat_renders_without_exact_proposals_and_when_all_are_filtered(
    live_server, page, tmp_path,
):
    """The summary stat/link must stay visible when the exact-byte section is empty.

    ``renderResults`` short-circuits to ``renderNoDuplicateGroups`` when
    there are no exact-byte proposals, and to ``renderAllGroupsFiltered``
    when every exact proposal is hidden by filters. Both paths route the
    same-shot stat through ``renderSameShotOnlySummaryBar`` and must keep
    the link to ``#sameShotResults`` and its count visible, without
    reviving the exact-byte apply bar.
    """
    db = live_server["db"]
    seed_same_shot_review(db, tmp_path)
    # Branch 1: no exact duplicates — only same-shot likely groups exist.
    page.set_viewport_size({"width": 1280, "height": 900})
    page.goto(f"{live_server['url']}/duplicates")
    stat = page.locator("#summary a.summary-stat-link")
    expect(stat).to_contain_text("1")
    expect(stat).to_contain_text("Same-shot to review")
    expect(page.locator("#applyBar")).to_be_hidden()
    expect(page.locator("#results")).to_contain_text("No exact-byte duplicates found")
    section = page.locator("#sameShotResults")
    expect(section.locator(".same-shot-group")).to_have_count(1)
    stat.click()
    expect(section).to_be_in_viewport()
    # Branch 2: exact-byte proposals exist but every one is filtered out,
    # while a same-shot member still matches the active photo filter.
    _add_photo(db, tmp_path / "exact-a", "copy.jpg", "exact",
               timestamp="2024-05-01T10:00:00")
    _add_photo(db, tmp_path / "exact-b", "copy.jpg", "exact",
               timestamp="2024-05-01T10:00:00")
    db.commit()
    refreshed = run_duplicate_scan({}, db)
    assert refreshed["proposals"], "branch 2 needs at least one exact-byte proposal to filter out"
    db.conn.execute("UPDATE job_history SET result=? WHERE id='same-shot-e2e'",
                    (json.dumps(refreshed),))
    db.commit()
    same_shot_photo_id = refreshed["same_shot_groups"][0]["photos"][0]["id"]
    page.reload()
    expect(stat).to_contain_text("1")
    # Hide every exact-byte group by filtering to a same-shot member id only.
    page.evaluate(
        "id => { duplicatesState.filterMatchedIds = new Set([id]);"
        " pageModules.results.renderResults(duplicatesState.lastScanResult); }",
        same_shot_photo_id,
    )
    expect(stat).to_contain_text("Same-shot to review")
    expect(page.locator("#applyBar")).to_be_hidden()
    expect(page.locator("#results")).to_contain_text(
        "No duplicate groups match the active filters"
    )
    expect(section.locator(".same-shot-group")).to_have_count(1)
    # Reviewing the sole likely group drops the live stat to 0 and leaves no
    # exact-byte control resurrected by the empty/filtered branches.
    section.get_by_role("button", name="Keep all copies").click()
    expect(stat.locator(".num")).to_have_text("0")
    expect(page.locator("#applyBar")).to_be_hidden()


def test_compare_save_reload_reopen_and_rescan(live_server, page, tmp_path):
    db = live_server["db"]
    a, b = seed_same_shot_review(db, tmp_path)
    page.set_viewport_size({"width": 1440, "height": 1100})
    page.goto(f"{live_server['url']}/duplicates")
    section = page.locator("#sameShotResults")
    section.get_by_role("button", name="Compare images").click()
    dialog = page.get_by_role("dialog", name="Compare possible same-shot copies")
    expect(dialog).to_be_visible()
    expect(dialog).to_contain_text("cached thumbnails")
    expect(dialog.locator("#sameShotSide0")).to_contain_text("2026-04-03")
    expect(dialog.locator("#sameShotSide1")).to_contain_text("2023-01-01")
    expect(dialog.locator(".same-shot-image-status")).to_have_text(["Cached thumbnail", "Cached thumbnail"])
    dialog.locator('[data-same-shot-side="0"]').select_option("1")
    expect(dialog.locator("#sameShotSide0")).to_contain_text("2023-01-01")
    dialog.locator('[data-same-shot-side="0"]').select_option("0")
    if screenshot := os.environ.get("VIREO_COMPARISON_SCREENSHOT"):
        Path(screenshot).parent.mkdir(parents=True, exist_ok=True)
        page.screenshot(path=screenshot)
    dialog.get_by_role("button", name="Keep all copies").click()
    expect(dialog).not_to_be_visible()
    expect(section.locator(".same-shot-group")).to_have_count(0)
    expect(section).to_contain_text("1 matching group reviewed")
    expect(section).to_contain_text("All matching groups have saved review decisions")
    page.reload()
    expect(section.locator(".same-shot-group")).to_have_count(0)
    section.get_by_role("button", name="Show reviewed groups").click()
    expect(section.locator(".same-shot-group")).to_have_count(1)
    expect(section.locator(".same-shot-review-note")).to_contain_text("Keep all copies")
    section.get_by_role("button", name="Reopen review").click()
    expect(section.get_by_role("button", name="Mark reviewed")).to_be_visible()
    section.get_by_role("button", name="Mark reviewed").click()
    expect(section.locator(".same-shot-review-note")).to_contain_text("Reviewed")
    # Rescanning preserves the decision when evidence stays the same.
    page.locator("#scanBtn").click()
    expect(section.locator(".same-shot-review-note")).to_contain_text("Reviewed")
    # Capture date changes invalidate the snapshot, even when file hashes remain unchanged.
    db.conn.execute("UPDATE photos SET timestamp='2026-10-25T01:02:03' WHERE id=?", (b,))
    db.commit()
    page.reload()
    expect(section.locator(".same-shot-group")).to_have_count(1)
    expect(section.get_by_role("button", name="Mark reviewed")).to_be_visible()
    assert {row[0] for row in db.conn.execute("SELECT flag FROM photos WHERE id IN (?,?)", (a, b))} != {"rejected"}


def test_compare_missing_thumbnail_and_failed_save_are_visible_and_retryable(live_server, page, tmp_path):
    seed_same_shot_review(live_server["db"], tmp_path)
    page.goto(f"{live_server['url']}/duplicates")
    page.route("**/thumbnails/duplicate/*", lambda route: route.fulfill(status=404))
    page.locator("#sameShotResults").get_by_role("button", name="Compare images").click()
    dialog = page.get_by_role("dialog")
    expect(dialog.locator(".same-shot-image-status")).to_have_text([
        "Thumbnail unavailable. Reveal the original to inspect it.",
        "Thumbnail unavailable. Reveal the original to inspect it.",
    ])
    page.route("**/api/duplicates/same-shot/review", lambda route: route.fulfill(
        status=409, json={"error": "same-shot group changed", "message": "This group changed. Reload or scan again before reviewing it."}))
    dialog.get_by_role("button", name="Mark reviewed").click()
    expect(dialog.locator("#sameShotCompareError")).to_contain_text("Could not save review")
    expect(dialog.get_by_role("button", name="Mark reviewed")).to_be_enabled()
    assert live_server["db"].conn.execute("SELECT COUNT(*) FROM same_shot_reviews").fetchone()[0] == 0
    page.unroute("**/api/duplicates/same-shot/review")
    dialog.get_by_role("button", name="Mark reviewed").click()
    expect(dialog).not_to_be_visible()
    expect(page.locator("#sameShotResults .same-shot-group")).to_have_count(0)


def test_saving_review_cannot_resurrect_a_filtered_group(live_server, page, tmp_path):
    seed_same_shot_review(live_server["db"], tmp_path)
    page.goto(f"{live_server['url']}/duplicates")
    delayed = []

    def hold_response(route):
        response = route.fetch()
        delayed.append((route, response.json()))

    page.route("**/api/duplicates/same-shot/review", hold_response)
    section = page.locator("#sameShotResults")
    section.get_by_role("button", name="Mark reviewed").click()
    expect(section.get_by_role("button", name="Mark reviewed")).to_be_disabled()
    expect(section).to_contain_text("Saving review")
    # Neither a scan nor a catalog reload may replace the snapshot during a save.
    assert page.evaluate("pageModules.restore.prepareDuplicateScan()") is False
    page.evaluate("pageModules.restore.tryRestoreLastScan(true)")
    expect(section.get_by_role("button", name="Mark reviewed")).to_be_disabled()
    page.evaluate("duplicatesState.filterMatchedIds = new Set(); pageModules.results.renderResults(duplicatesState.lastScanResult)")
    expect(section.locator(".same-shot-group")).to_have_count(0)
    assert len(delayed) == 1
    route, body = delayed[0]
    route.fulfill(json=body)
    expect(section.locator(".same-shot-group")).to_have_count(0)
    expect(section).to_contain_text("No possible same-shot groups match the active photo filters")


def test_compare_handles_more_than_two_copies_small_screens_and_escape(live_server, page, tmp_path):
    _, _, third = seed_same_shot_review(live_server["db"], tmp_path, count=3)
    page.set_viewport_size({"width": 600, "height": 900})
    page.goto(f"{live_server['url']}/duplicates")
    section = page.locator("#sameShotResults")
    section.get_by_role("button", name="Compare images").click()
    dialog = page.get_by_role("dialog")
    expect(dialog).to_be_visible()
    expect(dialog.locator('[data-same-shot-side="1"] option')).to_have_count(3)
    dialog.locator('[data-same-shot-side="1"]').select_option(label="DSC_0386.jpg · 2026-04-03T01:02:04 · Other version")
    expect(dialog.locator("#sameShotSide1 img")).to_have_attribute("src", f"/thumbnails/duplicate/{third}.jpg")
    assert dialog.evaluate("node => node.getBoundingClientRect().left >= 0 && node.getBoundingClientRect().right <= innerWidth")
    page.keyboard.press("Escape")
    expect(dialog).not_to_be_visible()
    section.get_by_role("button", name="Keep all copies").click()
    expect(section.locator(".same-shot-group")).to_have_count(0)
    page.reload()
    expect(section).to_contain_text("1 matching group reviewed")
    section.get_by_role("button", name="Show reviewed groups").click()
    expect(section.locator(".dup-card")).to_have_count(3)
    expect(section.locator(".badge")).to_have_text(["KEEP ALL"] * 3)
