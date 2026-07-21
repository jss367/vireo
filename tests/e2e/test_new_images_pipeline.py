"""E2E test for the new-images banner -> Import flow.

Covers Task 11 of docs/plans/2026-04-22-new-images-pipeline-plan.md:
drop a JPEG into a registered folder, see the banner, click "Review import",
end up on /import?new_images=<id> with the frozen files loaded, import in
place, and confirm the photo is visible on /browse.

Does not reuse the shared `live_server` fixture from conftest.py because that
one seeds phantom photos under /photos/park and /photos/yard that don't exist
on disk — which would make "new images" detection unreliable. Instead this
module spins up its own Flask server backed by an empty workspace and a real
temp folder.
"""
import os
import sys
import threading

import pytest
from PIL import Image
from playwright.sync_api import expect
from werkzeug.serving import make_server

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "..", "vireo"))


def _write_jpeg(path, size=(64, 64), color="red"):
    """Write a valid JPEG to `path`. Real bytes so `Pillow.open()` works."""
    Image.new("RGB", size, color=color).save(str(path), "JPEG")


@pytest.fixture()
def fresh_server(tmp_path, monkeypatch):
    """Start a Flask server against an empty workspace + temp photo folder.

    Returns: {"url", "db", "photo_dir"}.
    """
    monkeypatch.setenv("HOME", str(tmp_path))

    import config as cfg
    from app import create_app
    from db import Database

    monkeypatch.setattr(cfg, "CONFIG_PATH", str(tmp_path / "config.json"))

    db_path = str(tmp_path / "test.db")
    thumb_dir = str(tmp_path / "thumbs")
    os.makedirs(thumb_dir)

    photo_dir = tmp_path / "photos"
    photo_dir.mkdir()

    db = Database(db_path)
    ws_id = db.ensure_default_workspace()
    db.set_active_workspace(ws_id)
    # Register the folder so it's "known" to the workspace but empty on disk.
    folder_id = db.add_folder(str(photo_dir), name="photos")
    db.add_workspace_folder(ws_id, folder_id)

    app = create_app(db_path=db_path, thumb_cache_dir=thumb_dir)

    server = make_server("127.0.0.1", 0, app)
    port = server.socket.getsockname()[1]
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()

    yield {
        "url": f"http://127.0.0.1:{port}",
        "db": db,
        "photo_dir": photo_dir,
        "app": app,
    }

    server.shutdown()
    thread.join(timeout=5)


def _clear_new_images_cache():
    """Bust the in-process new-images cache so fresh disk state is observed.

    The cache sits in the `new_images` module and is shared by both
    `count_new_images_for_workspace` and the `/api/workspaces/active/new-images`
    endpoint. Tests that drop files on disk between page loads must clear it
    or the banner never appears.
    """
    from new_images import get_shared_cache
    get_shared_cache().clear()


def test_new_images_banner_drives_import(fresh_server, page):
    """Full user flow: drop file -> banner -> import -> photo visible."""
    url = fresh_server["url"]
    photo_dir = fresh_server["photo_dir"]
    db = fresh_server["db"]

    # --- Step 1: drop a JPEG into the registered folder. ---
    jpeg_path = photo_dir / "IMG_0001.JPG"
    _write_jpeg(jpeg_path)
    _clear_new_images_cache()

    # --- Step 2: visit any Vireo page; banner should appear. ---
    page.goto(f"{url}/browse")
    banner = page.locator("#newImagesBanner")
    expect(banner).to_be_visible(timeout=5000)
    msg = page.locator("#newImagesMsg")
    expect(msg).to_contain_text("1 new image")

    # --- Step 3: review the import and land on its frozen snapshot. ---
    page.locator("#newImagesBanner .banner-cta").click()
    page.wait_for_url("**/import?new_images=*", timeout=5000)
    assert "new_images=" in page.url

    # --- Step 4: snapshot mode is visible, exact, and fixed to Add in place. ---
    source_note = page.locator("#newImagesImportSource")
    expect(source_note).to_contain_text("1 newly detected image")
    expect(page.locator("#modeInPlace")).to_be_checked()
    expect(page.locator("#modeCopy")).to_be_disabled()
    expect(page.locator("#previewSummary")).to_contain_text("1 captured file")

    # --- Step 5: choose import-only and admit the captured photo. ---
    page.locator("#afterImportSelect").select_option("__none__")
    start_btn = page.locator("#btnStart")
    expect(start_btn).to_be_enabled(timeout=5000)
    start_btn.click()

    # --- Step 6: Import reports a durable catalog result. ---
    expect(page.locator("#resultCard")).to_be_visible(timeout=30000)
    expect(page.locator("#resultSummary")).to_contain_text("1 imported")

    # Sanity-check that the scan actually ingested the photo.
    photo_row = db.conn.execute(
        "SELECT id, filename FROM photos WHERE filename = ?",
        ("IMG_0001.JPG",),
    ).fetchone()
    assert photo_row is not None, "Import did not index IMG_0001.JPG"

    # --- Step 7: navigate to /browse and confirm the photo is visible. ---
    page.goto(f"{url}/browse")
    card = page.locator(".grid-card[data-filename='IMG_0001.JPG']")
    expect(card).to_be_visible(timeout=5000)


def test_banner_click_during_walk_shows_preparing_state(fresh_server, page, monkeypatch):
    """Regression test for the reported banner-click bug: reviewing an import
    while the server-side new-images walk is still running used to
    freeze the banner button for ~60s and then silently dump the user on a
    blank wizard. Now the click navigates immediately to the Import page in
    a visible "preparing" state that shows live walk
    progress and converges onto the snapshot once the walk finishes."""
    import threading

    import new_images as new_images_module
    from new_images import get_shared_cache

    url = fresh_server["url"]
    photo_dir = fresh_server["photo_dir"]

    _write_jpeg(photo_dir / "IMG_0002.JPG")
    _clear_new_images_cache()

    page.goto(f"{url}/browse")
    banner = page.locator("#newImagesBanner")
    expect(banner).to_be_visible(timeout=5000)

    # Simulate the real failure conditions: the cache is cold at click time
    # (as after a scan invalidation) and the walk is slow (as on a large
    # network volume). The walk reports progress, then blocks until the test
    # releases it — deterministic, no sleep races.
    release = threading.Event()
    real_count = new_images_module.count_new_images_for_workspace

    def slow_count(*args, **kwargs):
        cb = kwargs.get("progress_callback")
        if cb:
            cb(1500, 1)
        release.wait(timeout=15)
        return real_count(*args, **kwargs)

    monkeypatch.setattr(
        new_images_module, "count_new_images_for_workspace", slow_count,
    )
    get_shared_cache().clear()

    try:
        # Click lands on the Import page immediately — no frozen button.
        page.locator("#newImagesBanner .banner-cta").click()
        page.wait_for_url("**/import?new_images=preparing", timeout=5000)

        # Live walk progress is shown, not an opaque spinner.
        status = page.locator("#newImagesImportSource")
        expect(status).to_contain_text("1,500 files checked", timeout=10000)
    finally:
        release.set()

    # Once the walk finishes, the page converges onto the real snapshot:
    # URL rewritten to the id, subtitle shows the advertised count.
    page.wait_for_url(
        lambda u: "new_images=" in u and "preparing" not in u, timeout=15000,
    )
    expect(page.locator("#newImagesImportSource")).to_contain_text(
        "1 newly detected image", timeout=5000,
    )
