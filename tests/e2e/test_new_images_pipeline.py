"""E2E test for the new-images banner -> pipeline flow.

Covers Task 11 of docs/plans/2026-04-22-new-images-pipeline-plan.md:
drop a JPEG into a registered folder, see the banner, click "Create a
pipeline", end up on /pipeline?new_images=<id> with the "New images" source
card pre-selected, start the pipeline, and confirm the photo is visible on
/browse.

Does not reuse the shared `live_server` fixture from conftest.py because that
one seeds phantom photos under /photos/park and /photos/yard that don't exist
on disk — which would make "new images" detection unreliable. Instead this
module spins up its own Flask server backed by an empty workspace and a real
temp folder.
"""
import os
import sys
import threading
import time

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


def test_new_images_banner_drives_pipeline(fresh_server, page):
    """Full user flow: drop file -> banner -> pipeline -> photo visible."""
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

    # --- Step 3: click "Create a pipeline" and land on /pipeline?new_images=<id>. ---
    page.locator("#newImagesBanner .banner-cta").click()
    page.wait_for_url("**/pipeline?new_images=*", timeout=5000)
    assert "new_images=" in page.url

    # --- Step 4: the "New images" source card is visible AND selected. ---
    card = page.locator("#sourceOptionNewImages")
    expect(card).to_be_visible()
    radio = page.locator("[data-testid='source-new-images']")
    expect(radio).to_be_checked()

    # Subtitle shows the snapshot's count. The JS renders " \u2014 1 new image in 1 folder".
    subtitle = page.locator("#newImagesCardSubtitle")
    expect(subtitle).to_contain_text("1 new image")

    # --- Step 5: submit the pipeline with classify/extract/group disabled to
    # keep it fast (no model in this test environment anyway).
    # Un-check classify/extract/group to skip the heavy stages.
    for cb_id in ("enableClassify", "enableExtract", "enableGroup"):
        checkbox = page.locator(f"#{cb_id}")
        if checkbox.is_checked():
            checkbox.uncheck()

    start_btn = page.locator("[data-testid='start-pipeline-btn']")
    expect(start_btn).to_be_enabled(timeout=5000)
    start_btn.click()

    # --- Step 6: wait for the job to finish. The snapshot pipeline path runs
    # scan-only when all the optional stages are skipped, which is fast.
    # Poll the DB directly rather than scraping the UI.
    deadline = time.time() + 30
    photo_row = None
    while time.time() < deadline:
        photo_row = db.conn.execute(
            "SELECT id, filename FROM photos WHERE filename = ?",
            ("IMG_0001.JPG",),
        ).fetchone()
        if photo_row is not None:
            break
        time.sleep(0.25)
    assert photo_row is not None, (
        "Pipeline never ingested IMG_0001.JPG within 30s"
    )

    # --- Step 7: navigate to /browse and confirm the photo is visible. ---
    page.goto(f"{url}/browse")
    card = page.locator(".grid-card[data-filename='IMG_0001.JPG']")
    expect(card).to_be_visible(timeout=5000)
