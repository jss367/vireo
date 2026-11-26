"""Import page: cleaning up an old staging folder.

The cleanup unlinks files, so the page has to say what its check proved
(each deletable file is byte-identical to its archive copy), ask before it
deletes, and show what was deleted and what was kept.
"""
import re

from playwright.sync_api import expect
from services import photo_ingest, workspace_folders

# What tauri-plugin-dialog's init script does to window.confirm: the result
# is a promise, always truthy, and no dialog is shown.
DESKTOP_CONFIRM = "window.confirm = async function() { return true; };"

DIALOG = "#vireoConfirmDialog"
CANCEL = f"{DIALOG} [data-vireo-confirm=cancel]"
ACCEPT = f"{DIALOG} [data-vireo-confirm=accept]"


def _write(path, data):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(data)
    return path


def _archive_photo(db, tmp_path, name, data):
    """A cataloged archive file under ``archive/USA`` in the active workspace."""
    archive_file = _write(tmp_path / "archive" / "USA" / name, data)
    folder_id = workspace_folders.add_folder(db, str(archive_file.parent), name="USA")
    photo_ingest.add_photo(db, folder_id, name, ".nef", len(data), 1.0)
    db.commit()
    return archive_file


def _staging_row(page, root):
    row = page.locator(".staging-item").filter(has_text=root.name)
    row.wait_for(state="visible")
    return row


def test_cleanup_deletes_only_files_identical_to_the_archive(live_server, page, tmp_path):
    db = live_server["db"]
    root = tmp_path / "staging" / "pipeline-mixed"
    identical = _write(root / "USA" / "a.nef", b"same bytes")
    different = _write(root / "USA" / "b.nef", b"good bytes")
    stray = _write(root / "USA" / "notes.txt", b"only copy")
    _archive_photo(db, tmp_path, "a.nef", b"same bytes")
    # Same name and size as the staged file, different contents.
    archive_b = _archive_photo(db, tmp_path, "b.nef", b"bad! bytes")

    page.add_init_script(DESKTOP_CONFIRM)
    page.goto(f"{live_server['url']}/import")
    row = _staging_row(page, root)
    expect(row.locator(".staging-meta")).to_contain_text("3 files")
    expect(row.locator(".staging-meta")).to_contain_text("not compared with the archive yet")

    row.get_by_role("button", name="Compare with the archive").click()

    meta = row.locator(".staging-meta")
    expect(meta).to_contain_text("1 identical to the archive copy")
    expect(meta).to_contain_text("2 not in archive")
    details = row.locator(".staging-details li")
    expect(details.filter(has_text="b.nef")).to_contain_text("different contents")
    expect(details.filter(has_text="notes.txt")).to_contain_text("no cataloged archive photo")
    expect(row.get_by_role("button", name="Use as import source")).to_be_visible()

    delete = row.get_by_role("button", name="Delete the 1 identical file")
    delete.click()
    expect(page.locator(f"{DIALOG} h3")).to_have_text("Delete 1 staged file permanently?")
    question = page.locator(f"{DIALOG} .vireo-confirm-text")
    expect(question.nth(0)).to_contain_text("not moved to the Trash")
    expect(question.nth(1)).to_contain_text("byte for byte")
    expect(question.nth(2)).to_contain_text("2 other entries have no identical archive copy and stay")
    # Nothing is deleted while the question is open, or after it is declined.
    assert identical.exists()
    page.locator(CANCEL).click()
    expect(page.locator(DIALOG)).to_have_count(0)
    expect(delete).to_be_enabled()
    assert identical.exists()

    delete.click()
    page.locator(ACCEPT).click()

    expect(meta).to_contain_text(re.compile(r"Deleted 1 file \(.+\) after matching it with the archive copy"))
    expect(meta).to_contain_text("2 entries kept")
    expect(row.locator(".staging-details-heading")).to_have_text("Kept in the folder:")
    expect(row.locator(".staging-details li").filter(has_text="b.nef")).to_be_visible()
    expect(row.get_by_role("button", name="Compare again")).to_be_visible()
    assert not identical.exists()
    assert different.read_bytes() == b"good bytes"
    assert stray.read_bytes() == b"only copy"
    assert archive_b.read_bytes() == b"bad! bytes"


def test_cleanup_removes_a_folder_whose_files_all_match(live_server, page, tmp_path):
    db = live_server["db"]
    root = tmp_path / "staging" / "pipeline-clean"
    staged = _write(root / "USA" / "a.nef", b"same bytes")
    archive_file = _archive_photo(db, tmp_path, "a.nef", b"same bytes")

    page.goto(f"{live_server['url']}/import")
    row = _staging_row(page, root)
    row.get_by_role("button", name="Compare with the archive").click()
    expect(row.locator(".staging-meta")).to_contain_text("1 identical to the archive copy")
    expect(row.get_by_role("button", name="Use as import source")).to_have_count(0)

    row.get_by_role("button", name="Delete staging copy").click()
    expect(page.locator(f"{DIALOG} .vireo-confirm-text")).to_have_count(2)
    page.locator(ACCEPT).click()

    outcome = page.locator("#orphanedStagingItems .staging-outcome")
    expect(outcome).to_contain_text("pipeline-clean: Deleted 1 file")
    expect(outcome).to_contain_text("The folder was removed.")
    expect(page.locator(".staging-item")).to_have_count(0)
    assert not staged.exists()
    assert not root.exists()
    assert archive_file.read_bytes() == b"same bytes"
