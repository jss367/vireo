"""Same-shot candidates remain visible, filterable and strictly review-only."""
import json
import os
from pathlib import Path

from duplicate_scan import run_duplicate_scan
from PIL import Image, ImageDraw
from playwright.sync_api import expect
from services import photo_ingest


def seed_same_shot_review(db, tmp_path):
    active = db.active_workspace_id
    photos = []
    for index, date in enumerate(("2023-01-01T01:02:03", "2026-04-03T01:02:03")):
        if index:
            db.set_active_workspace(db.create_workspace("Corrected dates"))
        folder = tmp_path / ("Original camera date" if not index else "Corrected capture date")
        folder.mkdir()
        path = folder / "DSC_0386.jpg"
        image = Image.new("RGB", (480, 320), "#657a62")
        draw = ImageDraw.Draw(image)
        draw.ellipse((180, 95, 330, 220), fill="#c5a078")
        draw.ellipse((280, 70, 350, 130), fill="#c5a078")
        draw.polygon(((345, 94), (375, 105), (345, 110)), fill="#39352c")
        draw.ellipse((328, 87, 336, 95), fill="#222222")
        image.save(path)
        fid = db.add_folder(str(folder))
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
    page.evaluate("id => { _filterMatchedIds = new Set([id]); renderResults(_lastScanResult); }", a)
    expect(section.locator(".dup-card")).to_have_count(2)
    page.evaluate("_filterMatchedIds = new Set(); renderResults(_lastScanResult)")
    expect(section.locator(".same-shot-group")).to_have_count(0)
    expect(section).to_contain_text("No possible same-shot groups match the active photo filters")
    page.evaluate("_filterMatchedIds = null; renderResults(_lastScanResult)")
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
      const result = {..._lastScanResult};
      result.same_shot_groups = Array(51).fill(result.same_shot_groups[0]);
      renderResults(result);
    }""")
    expect(section.locator(".same-shot-group")).to_have_count(50)
    expect(section).to_contain_text("Showing 50 of 51 matching groups")
    section.locator("[data-same-shot-more]").click()
    expect(section.locator(".same-shot-group")).to_have_count(51)


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
