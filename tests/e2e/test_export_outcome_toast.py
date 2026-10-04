"""After Export is clicked, the user is told how the export job ended."""

import pytest
from PIL import Image
from playwright.sync_api import expect


@pytest.fixture
def field_photos(live_server, tmp_path):
    db = live_server["db"]
    folder = tmp_path / "field"
    folder.mkdir()
    folder_id = db.add_folder(str(folder))
    ids = []
    for index, name in enumerate(["kestrel-a.jpg", "kestrel-b.jpg"]):
        path = folder / name
        Image.new("RGB", (240, 160), (90 + index * 40, 110, 80)).save(path)
        ids.append(db.add_photo(
            folder_id=folder_id, filename=name, extension=".jpg",
            file_size=path.stat().st_size, file_mtime=path.stat().st_mtime,
            width=240, height=160,
        ))
    return {"ids": ids, "folder": folder}


def _open_browse_export(page, live_server, ids):
    page.goto(live_server["url"] + "/browse")
    for pid in ids:
        page.locator(f'.grid-card[data-id="{pid}"]').click(modifiers=["Meta"])
    page.locator("#batchBar").get_by_role("button", name="Export", exact=True).click()
    expect(page.locator("#exportOverlay")).to_have_class("modal-overlay open")


def test_browse_export_reports_where_the_photos_went(live_server, page, field_photos):
    _open_browse_export(page, live_server, field_photos["ids"])
    dest = field_photos["folder"].parent / "out"
    dest.mkdir()
    with page.expect_response(lambda r: r.url.endswith("/api/jobs/export/preflight")):
        page.locator("#exportDest").fill(str(dest))
    page.locator("#exportSubmitBtn").click()

    toast = page.locator('#toastContainer [data-type="success"]')
    expect(toast).to_have_text(f"Exported 2 photos to {dest}", timeout=15000)


def test_browse_export_reports_an_unreachable_original_folder(
    live_server, page, field_photos,
):
    _open_browse_export(page, live_server, field_photos["ids"][:1])
    folder = field_photos["folder"]
    # The drive holding the originals drops off while the modal is open.
    folder.rename(folder.parent / "unplugged")
    page.locator("#exportSubmitBtn").click()

    toast = page.locator('#toastContainer [data-type="error"]')
    expect(toast).to_have_text(
        "Nothing was exported. 1 photo failed: "
        f"kestrel-a.jpg: original folder is not reachable ({folder})",
        timeout=15000,
    )
    assert not folder.exists()


def test_photo_editor_export_reports_its_outcome(live_server, page, field_photos):
    pid = field_photos["ids"][0]
    page.goto(f"{live_server['url']}/edit/{pid}")
    expect(page.locator("#editorFilename")).to_have_text("kestrel-a.jpg")
    page.locator("#exportBtn").click()
    expect(page.locator("#exportOverlay")).to_have_class("modal-overlay open")
    folder = field_photos["folder"]
    folder.rename(folder.parent / "unplugged")
    page.locator("#exportSubmitBtn").click()

    toast = page.locator('#toastContainer [data-type="error"]')
    expect(toast).to_contain_text("original folder is not reachable", timeout=15000)
