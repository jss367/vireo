"""After Export is clicked, the user is told how the export job ended."""

import pytest
from PIL import Image
from playwright.sync_api import expect
from services import photo_ingest, workspace_folders


@pytest.fixture
def field_photos(live_server, tmp_path):
    db = live_server["db"]
    folder = tmp_path / "field"
    folder.mkdir()
    folder_id = workspace_folders.add_folder(db, str(folder))
    ids = []
    for index, name in enumerate(["kestrel-a.jpg", "kestrel-b.jpg"]):
        path = folder / name
        Image.new("RGB", (240, 160), (90 + index * 40, 110, 80)).save(path)
        ids.append(photo_ingest.add_photo(
            db, folder_id=folder_id, filename=name, extension=".jpg",
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


def _expect_toast(page, kind, summary):
    """Check the toast says ``summary``, open its notification details and
    return the full message shown there."""
    toast = page.locator(f'#toastContainer [data-type="{kind}"]')
    expect(toast.locator("span")).to_have_text(summary, timeout=15000)
    toast.get_by_role("button", name="View notification details").click()
    entry = page.locator("#notificationList details[open] p")
    expect(entry).to_have_count(1)
    return entry


def test_browse_export_reports_where_the_photos_went(live_server, page, field_photos):
    _open_browse_export(page, live_server, field_photos["ids"])
    dest = field_photos["folder"].parent / "out"
    dest.mkdir()
    with page.expect_response(lambda r: r.url.endswith("/api/jobs/export/preflight")):
        page.locator("#exportDest").fill(str(dest))
    page.locator("#exportSubmitBtn").click()

    details = _expect_toast(
        page, "success", "Exported 2 photos. No metadata selected.",
    )
    expect(details).to_have_text(
        f"Exported 2 photos to {dest}. Metadata: none selected (metadata-free export)"
    )


def test_browse_export_reports_an_unreachable_original_folder(
    live_server, page, field_photos,
):
    _open_browse_export(page, live_server, field_photos["ids"][:1])
    folder = field_photos["folder"]
    # The drive holding the originals drops off while the modal is open.
    folder.rename(folder.parent / "unplugged")
    page.locator("#exportSubmitBtn").click()

    details = _expect_toast(
        page, "error", "Nothing was exported. 1 photo failed. No metadata selected.",
    )
    expect(details).to_have_text(
        "Nothing was exported. 1 photo failed: "
        f"kestrel-a.jpg: original folder is not reachable ({folder})"
        ". Metadata: none selected (metadata-free export)"
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

    details = _expect_toast(
        page, "error", "Nothing was exported. 1 photo failed. No metadata selected.",
    )
    expect(details).to_contain_text("original folder is not reachable")
