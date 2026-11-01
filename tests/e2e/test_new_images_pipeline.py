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
import logging
import os
import sys
from contextlib import ExitStack
from pathlib import Path

import psutil
import pytest
from PIL import Image
from playwright.sync_api import expect

from e2e.threaded_server import start_server, stop_server

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "..", "vireo"))
from services import workspace_folders


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

    with ExitStack() as cleanup:
        db = cleanup.enter_context(Database(db_path))
        ws_id = db.ensure_default_workspace()
        db.commit()
        db.set_active_workspace(ws_id)
        # Register the folder so it's "known" to the workspace but empty on disk.
        folder_id = workspace_folders.add_folder(db, str(photo_dir), name="photos")
        workspace_folders.add_workspace_folder(db, ws_id, folder_id)

        app = create_app(db_path=db_path, thumb_cache_dir=thumb_dir)
        cleanup.callback(app._cleanup_app_resources)

        # Drain requests before closing app resources and the seed database.
        # Register cleanup as resources are acquired so setup failures also
        # release them, without waiting for cyclic garbage collection.
        server, thread, url = start_server(app)
        cleanup.callback(stop_server, server, thread)

        yield {
            "url": url,
            "db": db,
            "photo_dir": photo_dir,
            "app": app,
        }


@pytest.mark.parametrize("fail_to_start", [False, True])
def test_fresh_server_releases_resources(tmp_path, monkeypatch, fail_to_start):
    """Closing the fixture must release files even if the server cannot start."""
    apps = []
    real_start_server = start_server

    def capture_app(app):
        apps.append(app)
        if fail_to_start:
            raise OSError("server startup failed")
        return real_start_server(app)

    monkeypatch.setattr(sys.modules[__name__], "start_server", capture_app)
    # Drive setup/teardown directly so resources can be inspected afterwards.
    fixture = fresh_server.__wrapped__(tmp_path, monkeypatch)
    try:
        if fail_to_start:
            with pytest.raises(OSError, match="server startup failed"):
                next(fixture)
        else:
            data = next(fixture)
            with pytest.raises(StopIteration):
                next(fixture)
            assert data["db"].closed

        assert apps[0]._log_broadcaster not in logging.getLogger().handlers
        db_path = (tmp_path / "test.db").resolve()
        database_files = {db_path, Path(f"{db_path}-wal"), Path(f"{db_path}-shm")}
        assert not [
            f.path for f in psutil.Process().open_files()
            if Path(f.path).resolve() in database_files
        ]
    finally:
        fixture.close()
        for app in apps:
            app._cleanup_app_resources()


def _clear_new_images_cache():
    """Bust the in-process new-images cache so fresh disk state is observed.

    The cache sits in the `new_images` module and is shared by both
    `count_new_images_for_workspace` and the `/api/workspaces/active/new-images`
    endpoint. Tests that drop files on disk between page loads must clear it
    or the banner never appears.
    """
    from new_images_cache import get_shared_cache
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
    from new_images_cache import get_shared_cache

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


class _OfflineGate:
    """Reachability stand-in: every path under ``offline_prefix`` is on a
    volume that is unreachable; everything else is fine."""

    def __init__(self, offline_prefix):
        self.offline_prefix = str(offline_prefix)
        self.marked = []

    def check(self, path):
        if str(path).startswith(self.offline_prefix):
            return "/Volumes/NAS", False
        return None, True

    def mark_offline(self, root):
        self.marked.append(root)


def test_banner_reports_offline_folder_instead_of_failing(fresh_server, page, monkeypatch):
    """When a registered folder's volume is offline, the user sees a banner
    naming the folder as offline and unchecked — not a silent nothing and
    not a failed job. There is nothing to import, so no import button."""
    import volume_reachability

    url = fresh_server["url"]
    photo_dir = fresh_server["photo_dir"]
    _write_jpeg(photo_dir / "IMG_0003.JPG")
    monkeypatch.setattr(
        volume_reachability, "get_shared", lambda: _OfflineGate(photo_dir),
    )
    _clear_new_images_cache()

    page.goto(f"{url}/browse")
    banner = page.locator("#newImagesBanner")
    expect(banner).to_be_visible(timeout=5000)
    msg = page.locator("#newImagesMsg")
    expect(msg).to_contain_text("Couldn't check for new images")
    expect(msg).to_contain_text(str(photo_dir))
    expect(msg).to_contain_text("offline")
    expect(page.locator("#newImagesBanner .banner-cta")).to_be_hidden()

    # The walk itself completed rather than failing: no failed job.
    jobs = fresh_server["app"]._job_runner.list_jobs()
    walks = [j for j in jobs if j.get("type") == "new_images_walk"]
    assert walks and all(j.get("status") != "failed" for j in walks), walks

    # Dismissing sticks for this exact offline set across a reload.
    page.locator("#newImagesBanner .banner-dismiss").click()
    expect(banner).to_be_hidden()
    page.reload()
    page.wait_for_load_state("networkidle")
    expect(banner).to_be_hidden()


def test_banner_count_discloses_unchecked_offline_folder(fresh_server, page, monkeypatch):
    """A count from the reachable folders is shown, but the banner says which
    folder was offline and not checked, so a partial count is never read as
    the whole library."""
    import volume_reachability

    url = fresh_server["url"]
    db = fresh_server["db"]
    photo_dir = fresh_server["photo_dir"]
    nas_dir = photo_dir.parent / "nas"
    nas_dir.mkdir()
    _write_jpeg(nas_dir / "REMOTE.JPG")
    _write_jpeg(photo_dir / "LOCAL.JPG")
    ws_id = db._active_workspace_id
    nas_id = workspace_folders.add_folder(db, str(nas_dir), name="nas")
    workspace_folders.add_workspace_folder(db, ws_id, nas_id)

    monkeypatch.setattr(
        volume_reachability, "get_shared", lambda: _OfflineGate(nas_dir),
    )
    _clear_new_images_cache()

    page.goto(f"{url}/browse")
    banner = page.locator("#newImagesBanner")
    expect(banner).to_be_visible(timeout=5000)
    msg = page.locator("#newImagesMsg")
    expect(msg).to_contain_text("1 new image detected")
    expect(msg).to_contain_text(f"{nas_dir} is offline and not checked")
    expect(page.locator("#newImagesBanner .banner-cta")).to_be_visible()

    # Dismissing the mixed banner keeps it dismissed: it must not come back
    # as the offline-only notice on the next poll.
    page.locator("#newImagesBanner .banner-dismiss").click()
    expect(banner).to_be_hidden()
    page.reload()
    page.wait_for_load_state("networkidle")
    expect(banner).to_be_hidden()


def test_dismissed_count_rearms_when_a_folder_goes_offline(fresh_server, page, monkeypatch):
    """Dismissing "1 new image" must not also hide a later "1 new image, and
    one folder is offline" — the offline set is part of what was dismissed."""
    import volume_reachability

    url = fresh_server["url"]
    db = fresh_server["db"]
    photo_dir = fresh_server["photo_dir"]
    nas_dir = photo_dir.parent / "nas"
    nas_dir.mkdir()
    _write_jpeg(photo_dir / "LOCAL.JPG")
    ws_id = db._active_workspace_id
    nas_id = workspace_folders.add_folder(db, str(nas_dir), name="nas")
    workspace_folders.add_workspace_folder(db, ws_id, nas_id)
    _clear_new_images_cache()

    page.goto(f"{url}/browse")
    banner = page.locator("#newImagesBanner")
    expect(banner).to_be_visible(timeout=5000)
    expect(page.locator("#newImagesMsg")).to_contain_text("1 new image detected")
    page.locator("#newImagesBanner .banner-dismiss").click()
    expect(banner).to_be_hidden()

    # Same reachable count, but the second folder's volume drops.
    monkeypatch.setattr(
        volume_reachability, "get_shared", lambda: _OfflineGate(nas_dir),
    )
    _clear_new_images_cache()
    page.reload()
    expect(banner).to_be_visible(timeout=5000)
    expect(page.locator("#newImagesMsg")).to_contain_text("offline and not checked")


class _RemountableGate(_OfflineGate):
    """Offline gate for a share that can come back mid-test."""

    def __init__(self, offline_prefix):
        super().__init__(offline_prefix)
        self.online = False

    def check(self, path):
        if self.online:
            return None, True
        return super().check(path)


def test_offline_banner_offers_a_manual_recheck(fresh_server, page, monkeypatch):
    """A user who just remounted the share can click "Check again" and get
    the real count now, instead of waiting out the invisible 30s caches that
    the automatic poll relies on."""
    import volume_reachability

    url = fresh_server["url"]
    photo_dir = fresh_server["photo_dir"]
    _write_jpeg(photo_dir / "IMG_0005.JPG")
    gate = _RemountableGate(photo_dir)
    monkeypatch.setattr(volume_reachability, "get_shared", lambda: gate)
    _clear_new_images_cache()

    page.goto(f"{url}/browse")
    expect(page.locator("#newImagesBanner")).to_be_visible(timeout=5000)
    msg = page.locator("#newImagesMsg")
    expect(msg).to_contain_text("Couldn't check for new images")
    # Every full path stays available even when the sentence abbreviates.
    expect(msg).to_have_attribute("title", str(photo_dir))
    # The walk behind the notice is timestamped, so a recheck that finds the
    # volume still offline is distinguishable from a frozen banner.
    expect(page.locator("#newImagesCheckedAt")).to_contain_text("checked")

    recheck = page.locator("#newImagesRecheck")
    expect(recheck).to_be_visible()

    gate.online = True
    recheck.click()
    expect(msg).to_contain_text("1 new image", timeout=15000)
    expect(recheck).to_be_hidden()
    expect(page.locator("#newImagesCheckedAt")).to_have_text("")


def test_check_again_is_not_swallowed_by_an_in_flight_poll(fresh_server, page, monkeypatch):
    """Clicking "Check again" while the 60s poll is mid-request must still
    produce a post-invalidation walk. The in-flight poll was issued before
    the recheck cleared the caches, so its answer predates the click; without
    a queued re-poll the banner would sit on the stale offline notice until
    the next 60s tick."""
    import volume_reachability

    url = fresh_server["url"]
    photo_dir = fresh_server["photo_dir"]
    _write_jpeg(photo_dir / "IMG_0006.JPG")
    gate = _RemountableGate(photo_dir)
    monkeypatch.setattr(volume_reachability, "get_shared", lambda: gate)
    _clear_new_images_cache()

    page.goto(f"{url}/browse")
    msg = page.locator("#newImagesMsg")
    expect(msg).to_contain_text("Couldn't check for new images", timeout=5000)

    # Hold the *response* of the next navbar poll: the request goes out (and
    # is answered with the offline state) but the client does not see it
    # until the test releases it.
    page.evaluate("""() => {
      const realFetch = window.fetch;
      window.__hold = true;
      window.__release = null;
      window.fetch = (u, o) => {
        const pending = realFetch(u, o);
        if (window.__hold && String(u).includes('new-images')
            && !new URL(u, window.location.href).pathname.endsWith('/new-images/recheck')) {
          return new Promise(resolve => {
            window.__release = () => { window.__hold = false; resolve(pending); };
          });
        }
        return pending;
      };
    }""")
    page.evaluate("void checkNewImages()")
    page.wait_for_function("() => window.__release !== null", timeout=5000)

    gate.online = True
    page.locator("#newImagesRecheck").click()
    # The recheck landed while the poll above was still open, so the button
    # stays busy rather than being released by the stale answer.
    expect(page.locator("#newImagesRecheck")).to_be_disabled()

    page.evaluate("window.__release()")
    expect(msg).to_contain_text("1 new image", timeout=15000)


def test_check_again_releases_the_button_when_the_recheck_fails(
    fresh_server, page, monkeypatch,
):
    """A rejected recheck POST clears nothing, so polling would redraw the
    same cached answer and read as "checked again, still offline". The button
    comes back instead, so the click can be retried."""
    import volume_reachability

    url = fresh_server["url"]
    photo_dir = fresh_server["photo_dir"]
    _write_jpeg(photo_dir / "IMG_0007.JPG")
    gate = _RemountableGate(photo_dir)
    monkeypatch.setattr(volume_reachability, "get_shared", lambda: gate)
    _clear_new_images_cache()

    page.goto(f"{url}/browse")
    msg = page.locator("#newImagesMsg")
    expect(msg).to_contain_text("Couldn't check for new images", timeout=5000)

    page.evaluate("""() => {
      const realFetch = window.fetch;
      window.fetch = (u, o) => (new URL(u, window.location.href).pathname.endsWith('/new-images/recheck')
        ? Promise.resolve(new Response('', {status: 500}))
        : realFetch(u, o));
    }""")
    # Even with the volume back, a failed recheck must not be reported as a
    # completed check: nothing was invalidated, so the cached answer stands.
    gate.online = True
    page.locator("#newImagesRecheck").click()
    expect(page.locator("#newImagesCheckedAt")).to_contain_text(
        "recheck failed", timeout=5000,
    )
    expect(page.locator("#newImagesRecheck")).to_be_enabled()
    expect(msg).to_contain_text("Couldn't check for new images")


def test_check_again_releases_the_button_when_the_follow_up_poll_fails(
    fresh_server, page, monkeypatch,
):
    """The recheck POST can succeed and the poll behind it still fail. The
    button must not sit on "Checking..." until some later 60s tick happens to
    return a final payload."""
    import volume_reachability

    url = fresh_server["url"]
    photo_dir = fresh_server["photo_dir"]
    _write_jpeg(photo_dir / "IMG_0008.JPG")
    gate = _RemountableGate(photo_dir)
    monkeypatch.setattr(volume_reachability, "get_shared", lambda: gate)
    _clear_new_images_cache()

    page.goto(f"{url}/browse")
    msg = page.locator("#newImagesMsg")
    expect(msg).to_contain_text("Couldn't check for new images", timeout=5000)

    # The POST goes through; only the GET behind it is rejected.
    page.evaluate("""() => {
      const realFetch = window.fetch;
      window.fetch = (u, o) => (
        String(u).includes('new-images') && !new URL(u, window.location.href).pathname.endsWith('/new-images/recheck')
          ? Promise.resolve(new Response('', {status: 500}))
          : realFetch(u, o));
    }""")
    page.locator("#newImagesRecheck").click()
    expect(page.locator("#newImagesCheckedAt")).to_contain_text(
        "recheck failed", timeout=5000,
    )
    expect(page.locator("#newImagesRecheck")).to_be_enabled()


def test_a_poll_landing_mid_recheck_does_not_release_the_button(
    fresh_server, page, monkeypatch,
):
    """A 60s poll that was already in flight when "Check again" was clicked
    answers a question from before the invalidation. Rendering it must not
    re-enable the button — the recheck is still running, and an enabled
    button invites a duplicate invalidation."""
    import volume_reachability

    url = fresh_server["url"]
    photo_dir = fresh_server["photo_dir"]
    _write_jpeg(photo_dir / "IMG_0009.JPG")
    gate = _RemountableGate(photo_dir)
    monkeypatch.setattr(volume_reachability, "get_shared", lambda: gate)
    _clear_new_images_cache()

    page.goto(f"{url}/browse")
    msg = page.locator("#newImagesMsg")
    expect(msg).to_contain_text("Couldn't check for new images", timeout=5000)

    # Hold both the next poll's response and the recheck POST, so the stale
    # poll can be made to land first — while the POST is still open.
    page.evaluate("""() => {
      const realFetch = window.fetch;
      window.__releaseGet = null;
      window.__releasePost = null;
      window.fetch = (u, o) => {
        const pending = realFetch(u, o);
        if (new URL(u, window.location.href).pathname.endsWith('/new-images/recheck')) {
          return new Promise(resolve => {
            window.__releasePost = () => resolve(pending);
          });
        }
        if (String(u).includes('new-images') && window.__releaseGet === null) {
          return new Promise(resolve => {
            window.__releaseGet = () => resolve(pending);
          });
        }
        return pending;
      };
    }""")
    page.evaluate("void checkNewImages()")
    page.wait_for_function("() => window.__releaseGet !== null", timeout=5000)

    gate.online = True
    recheck = page.locator("#newImagesRecheck")
    recheck.click()
    page.wait_for_function("() => window.__releasePost !== null", timeout=5000)

    # The pre-invalidation poll lands while the POST is still open.
    page.evaluate("window.__releaseGet()")
    page.wait_for_timeout(300)
    expect(recheck).to_be_disabled()

    # Once the invalidation lands, the recheck's own poll concludes it.
    page.evaluate("window.__releasePost()")
    expect(msg).to_contain_text("1 new image", timeout=15000)
