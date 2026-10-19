from pathlib import Path

import pytest
from PIL import Image
from playwright.sync_api import expect
from services import photo_ingest


@pytest.fixture
def cached_photo(live_server, tmp_path):
    from offline_cache import cache_photo_original

    db = live_server["db"]
    folder = tmp_path / "source"
    folder.mkdir()
    source = folder / "bird.jpg"
    Image.new("RGB", (100, 100), "green").save(source)
    folder_id = db.add_folder(str(folder), name="Release test photos")
    pid = photo_ingest.add_photo(db, folder_id, source.name, ".jpg", file_size=source.stat().st_size, file_mtime=source.stat().st_mtime)
    thumb_dir = Path(live_server["app"].config["THUMB_CACHE_DIR"])
    Image.new("RGB", (100, 100), "green").save(thumb_dir / f"{pid}.jpg")
    base = thumb_dir.parent
    result = cache_photo_original(db, db.photos.get(pid), str(base), {folder_id: str(folder)})
    return {"id": pid, "folder_id": folder_id, "source": source, "cached": Path(result["path"])}


def test_storage_releases_all_cached_originals(live_server, page, cached_photo):
    errors = []
    page.on("pageerror", lambda error: errors.append(str(error)))
    page.goto(f"{live_server['url']}/storage")
    button = page.get_by_role("button", name="Release local originals…")
    expect(button).to_be_visible()
    expect(page.locator("#storageGrid")).to_contain_text("Release when done")
    dialogs = []

    def accept(dialog):
        dialogs.append(dialog.message)
        dialog.accept()

    page.once("dialog", accept)
    button.click()
    expect(page.locator("#toastContainer")).to_contain_text("1 local originals released")
    expect(page.locator("#storageGrid")).not_to_contain_text("Offline Originals")
    assert "every workspace" in dialogs[0]
    assert cached_photo["source"].is_file()
    assert not cached_photo["cached"].exists()
    assert not errors


@pytest.mark.parametrize("scope", ["selection", "folder"])
def test_browse_releases_originals_from_context_menu(live_server, page, cached_photo, scope):
    page.goto(f"{live_server['url']}/browse")
    if scope == "selection":
        page.locator(f'.grid-card[data-id="{cached_photo["id"]}"]').click(button="right")
    else:
        page.locator(f'.tree-item[data-folder-id="{cached_photo["folder_id"]}"]').click(button="right")
    item = page.locator(".vireo-ctx-menu").get_by_text("Release Local Originals…", exact=True)
    expect(item).to_be_visible()
    page.once("dialog", lambda dialog: dialog.accept())
    item.click()
    expect(page.locator("#toastContainer")).to_contain_text("1 local originals released")
    assert cached_photo["source"].is_file()
    assert not cached_photo["cached"].exists()
