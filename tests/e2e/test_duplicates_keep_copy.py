"""E2E coverage for "Keep this copy instead" on a resolved duplicate group."""

from playwright.sync_api import expect
from services import photo_ingest


def _seed_resolved_pair(db, root):
    """Two real copies of one file; the add_photo hook rejects the second.

    Returns ``(kept_id, rejected_id)``.
    """
    ids = []
    for folder_name in ("Early Australia", "Red Kangaroos"):
        folder = root / folder_name
        folder.mkdir(parents=True)
        (folder / "kangaroo.jpg").write_bytes(b"same bytes")
        fid = db.add_folder(str(folder))
        ids.append(photo_ingest.add_photo(
            db, folder_id=fid, filename="kangaroo.jpg", extension=".jpg",
            file_size=10, file_mtime=100.0, file_hash="KEEPE2E",
        ))
    flags = {
        r["id"]: r["flag"] for r in db.conn.execute(
            "SELECT id, flag FROM photos WHERE file_hash = 'KEEPE2E'"
        ).fetchall()
    }
    kept, rejected = sorted(ids, key=lambda pid: flags[pid] == "rejected")
    assert flags[rejected] == "rejected" and flags[kept] != "rejected"
    return kept, rejected


def test_keep_this_copy_instead_swaps_keep_and_rejected(live_server, page, tmp_path):
    db = live_server["db"]
    kept, rejected = _seed_resolved_pair(db, tmp_path / "library")

    page.goto(f"{live_server['url']}/duplicates")
    group = page.locator(".dup-group[data-hash='KEEPE2E']")
    old_keep = group.locator(f".dup-card[data-photo-id='{kept}']")
    chosen = group.locator(f".dup-card[data-photo-id='{rejected}']")
    expect(old_keep.locator(".badge")).to_have_text("KEEP")
    expect(old_keep.locator(".keep-btn")).to_have_count(0)
    expect(chosen.locator(".badge")).to_have_text("REJECTED")

    chosen.locator(".keep-btn").click()

    expect(chosen.locator(".badge")).to_have_text("KEEP")
    expect(chosen.locator(".trash-btn")).to_have_count(0)
    expect(old_keep.locator(".badge")).to_have_text("REJECTED")
    expect(old_keep.locator(".reason")).to_have_text("kept another copy instead")
    expect(old_keep.locator(".keep-btn")).to_be_visible()
    expect(old_keep.locator(".trash-btn")).to_be_visible()
    flags = {
        r["id"]: r["flag"] for r in db.conn.execute(
            "SELECT id, flag FROM photos WHERE file_hash = 'KEEPE2E'"
        ).fetchall()
    }
    assert flags == {kept: "rejected", rejected: "none"}

    # A reload reads the catalog, so the choice holds.
    page.reload()
    expect(chosen.locator(".badge")).to_have_text("KEEP")
    expect(old_keep.locator(".badge")).to_have_text("REJECTED")
