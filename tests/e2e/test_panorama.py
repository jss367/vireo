"""Browse selection through a real background stitch and saved image."""

import re

import cv2
import numpy as np
from PIL import Image
from playwright.sync_api import expect


def test_panorama_selection_folder_picker_and_real_stitch(live_server, page, tmp_path):
    db = live_server["db"]
    folder = tmp_path / "Panorama photos"
    folder.mkdir()
    fid = db.add_folder(str(folder))
    rng = np.random.default_rng(42)
    scene = cv2.GaussianBlur(rng.integers(0, 256, (300, 900, 3), dtype=np.uint8), (5, 5), 0)
    ids = []
    for i, pixels in enumerate((scene[:, :600], scene[:, 300:])):
        path = folder / f"landscape{i}.png"
        Image.fromarray(pixels).save(path)
        ids.append(
            db.add_photo(
                folder_id=fid,
                filename=path.name,
                extension=".png",
                file_size=path.stat().st_size,
                file_mtime=path.stat().st_mtime,
                width=600,
                height=300,
                timestamp="2026-09-19T10:00:00",
            )
        )

    page.goto(f"{live_server['url']}/browse")
    cards = [page.locator(f'.grid-card[data-id="{pid}"]') for pid in ids]
    cards[0].click(button="right")
    item = page.locator(".vireo-ctx-menu").get_by_text("Create Panorama…", exact=True)
    expect(item).to_have_class(re.compile("vireo-ctx-disabled"))
    page.keyboard.press("Escape")
    page.evaluate("clearSelection()")
    for card in cards:
        card.click(modifiers=["Meta"])
    page.locator("#batchMoreBtn").click()
    page.locator(".vireo-ctx-menu").get_by_text("Create Panorama…", exact=True).click()
    expect(page.locator("#panoramaSelection")).to_have_text("2 photos selected")

    destination = page.locator("#panoramaDestination")
    destination.fill(str(folder))
    page.locator("#panoramaBrowse").click()
    expect(page.locator("#folderBrowserTitle")).to_have_text("Choose Panorama Folder")
    expect(page.locator("#folderBrowserPath")).to_have_text(str(folder))
    page.locator("#folderBrowserSelectBtn").click()
    expect(destination).to_have_value(str(folder))
    expect(page.locator("#exportDest")).to_have_value("")

    page.locator("#panoramaFormat").select_option("png")
    page.locator("#panoramaReveal").uncheck()
    with page.expect_request("**/api/jobs/panorama") as launched:
        page.locator("#panoramaSubmit").click()
    assert launched.value.post_data_json["photo_ids"] == ids
    expect(page.locator("#panoramaStatus")).to_contain_text("Saved", timeout=30000)
    output = next(folder.glob("*_panorama.png"))
    with Image.open(output) as img:
        assert img.width > 800
    expect(page.locator("#panoramaStatus")).to_contain_text(str(output))
    expect(page.locator("#panoramaSubmit")).to_be_enabled()


def test_panorama_api_failure_keeps_selection_and_allows_retry(live_server, page):
    page.goto(f"{live_server['url']}/browse")
    cards = page.locator(".grid-card")
    cards.first.wait_for(state="visible")
    for index in (0, 1):
        cards.nth(index).click(modifiers=["Meta"])
    page.locator("#batchMoreBtn").click()
    page.locator(".vireo-ctx-menu").get_by_text("Create Panorama…", exact=True).click()
    page.route(
        "**/api/jobs/panorama",
        lambda route: route.fulfill(
            status=400,
            json={"error": "Choose an existing destination folder"},
        ),
    )
    page.locator("#panoramaSubmit").click()
    expect(page.locator("#panoramaStatus")).to_contain_text("Choose an existing destination folder")
    expect(page.locator("#panoramaSubmit")).to_be_enabled()
    expect(page.locator("#panoramaSelection")).to_have_text("2 photos selected")
    page.keyboard.press("Escape")
    expect(page.locator("#panoramaOverlay")).not_to_be_visible()
