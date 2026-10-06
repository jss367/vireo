"""The export modal's preflight: where files land, and numbered filenames
only when a name is taken."""

from pathlib import Path

import pytest
from PIL import Image
from playwright.sync_api import expect

PREFLIGHT = "/api/jobs/export/preflight"


@pytest.fixture
def export_photos(live_server, tmp_path):
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
    empty = tmp_path / "empty-dest"
    empty.mkdir()
    busy = tmp_path / "busy-dest"
    busy.mkdir()
    (busy / "kestrel-a.jpg").write_bytes(b"existing export")
    return {"ids": ids, "folder": folder, "empty": empty, "busy": busy}


def _open_browse_export(page, live_server, ids):
    page.goto(live_server["url"] + "/browse")
    for pid in ids:
        page.locator(f'.grid-card[data-id="{pid}"]').click(modifiers=["Meta"])
    page.locator("#batchBar").get_by_role("button", name="Export", exact=True).click()
    expect(page.locator("#exportOverlay")).to_have_class("modal-overlay open")


def _set_destination(page, path):
    with page.expect_response(lambda r: r.url.endswith(PREFLIGHT)):
        page.locator("#exportDest").fill(str(path))


def _wait_for_files(paths, page):
    deadline_ms = 15000
    waited = 0
    while not all(Path(p).exists() for p in paths) and waited < deadline_ms:
        page.wait_for_timeout(200)
        waited += 200
    missing = [str(p) for p in paths if not Path(p).exists()]
    assert not missing, f"export never wrote {missing}"


def test_browse_notice_appears_only_for_taken_names(live_server, page, export_photos):
    _open_browse_export(page, live_server, export_photos["ids"])
    notice = page.locator("#exportCollisionNotice")

    # With no custom destination, exports land next to the JPEG originals,
    # whose names are taken.
    expect(notice).to_contain_text("2 filenames are already taken")
    expect(notice).to_contain_text("kestrel-a.jpg → kestrel-a_2.jpg")
    expect(notice).to_contain_text("Nothing is overwritten.")

    _set_destination(page, export_photos["empty"])
    expect(notice).to_be_hidden()

    _set_destination(page, export_photos["busy"])
    expect(notice).to_contain_text("1 filename is already taken")
    expect(notice).to_contain_text("kestrel-a.jpg → kestrel-a_2.jpg")
    expect(notice).not_to_contain_text("kestrel-b.jpg")
    Path(".context").mkdir(exist_ok=True)
    page.locator("#exportOverlay .export-modal").screenshot(
        path=".context/export-collision-notice.png",
    )

    # The notice already showed the rename, so Export starts right away.
    page.locator("#exportSubmitBtn").click()
    expect(page.locator("#exportOverlay")).not_to_have_class("modal-overlay open")
    busy = export_photos["busy"]
    _wait_for_files([busy / "kestrel-a_2.jpg", busy / "kestrel-b.jpg"], page)
    assert (busy / "kestrel-a.jpg").read_bytes() == b"existing export"


def test_browse_export_stops_for_a_name_taken_after_the_check(
    live_server, page, export_photos,
):
    _open_browse_export(page, live_server, export_photos["ids"])
    notice = page.locator("#exportCollisionNotice")
    dest = export_photos["empty"]
    _set_destination(page, dest)
    expect(notice).to_be_hidden()

    # Another app writes a same-named file while the modal is open.
    (dest / "kestrel-b.jpg").write_bytes(b"arrived later")
    page.locator("#exportSubmitBtn").click()

    expect(notice).to_contain_text("1 filename is already taken")
    expect(notice).to_contain_text("kestrel-b.jpg → kestrel-b_2.jpg")
    expect(page.locator("#exportOverlay")).to_have_class("modal-overlay open")
    expect(page.locator("#exportSubmitBtn")).to_have_text("Export 2 photos")
    expect(page.locator("#exportSubmitBtn")).to_be_enabled()
    expect(page.locator("#exportDest")).to_be_enabled()
    assert not (dest / "kestrel-a.jpg").exists()

    page.locator("#exportSubmitBtn").click()
    expect(page.locator("#exportOverlay")).not_to_have_class("modal-overlay open")
    _wait_for_files([dest / "kestrel-a.jpg", dest / "kestrel-b_2.jpg"], page)
    assert (dest / "kestrel-b.jpg").read_bytes() == b"arrived later"


def test_browse_preview_shows_where_files_land(live_server, page, export_photos):
    _open_browse_export(page, live_server, export_photos["ids"])
    location = page.locator("#exportLocation")

    # No custom destination: next to the originals.
    expect(location).to_have_text(f"Location: {export_photos['folder']}")

    _set_destination(page, export_photos["empty"])
    expect(location).to_have_text(f"Location: {export_photos['empty']}")

    # The subfolder shows in the filename preview, under the same location.
    page.locator("#exportSubfolder").check()
    expect(page.locator("#exportPreview")).to_have_text("Preview: exported/kestrel-a.jpg")
    expect(location).to_have_text(f"Location: {export_photos['empty']}")
    Path(".context").mkdir(exist_ok=True)
    page.locator("#exportOverlay .export-summary").screenshot(
        path=".context/export-location.png",
    )

    _set_destination(page, "relative/folder")
    expect(location).to_have_text(
        "Location unavailable: destination must be an absolute path",
    )


def test_photo_editor_notice_follows_destination(live_server, page, export_photos):
    pid = export_photos["ids"][0]
    page.goto(f"{live_server['url']}/edit/{pid}")
    expect(page.locator("#editorFilename")).to_have_text("kestrel-a.jpg")
    page.locator("#exportBtn").click()
    expect(page.locator("#exportOverlay")).to_have_class("modal-overlay open")
    notice = page.locator("#exportCollisionNotice")

    expect(notice).to_contain_text("1 filename is already taken")
    expect(notice).to_contain_text("kestrel-a.jpg → kestrel-a_2.jpg")
    location = page.locator("#exportLocation")
    expect(location).to_have_text(f"Location: {export_photos['folder']}")

    _set_destination(page, export_photos["empty"])
    expect(notice).to_be_hidden()
    expect(location).to_have_text(f"Location: {export_photos['empty']}")
