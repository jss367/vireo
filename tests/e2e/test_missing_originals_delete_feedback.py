"""Missing Originals removal feedback in the navbar's modal.

The modal is navbar code (an ES module), so the tests answer its requests
(``/api/photos/missing/remove``, the ``/check`` that starts a refresh, and the
``/api/photos/missing`` status read) and read the toasts the user sees.
"""
from playwright.sync_api import expect

from e2e.browser_doubles import record_toasts, toasts

READY = {"status": "ready", "photos": []}
REMOVED = {"deleted": 2, "restored": [], "folder_offline": [], "skipped": 0}


class MissingOriginalsServer:
    """Routes for the endpoints removal and its refresh call.

    ``remove`` is a response dict, an HTTP status to fail with, or None to
    hold the request in ``held_removes`` until the test answers it.
    ``check_status`` fails the refresh's ``/check`` when not 200;
    ``status_payload`` is what the ``/api/photos/missing`` read returns."""

    def __init__(self, page):
        self.remove = REMOVED
        self.held_removes = []
        self.remove_calls = 0
        self.check_calls = 0
        self.check_status = 200
        self.check_payload = {"status": "ready"}
        self.status_payload = READY
        page.route("**/api/photos/missing/remove", self._remove)
        page.route("**/api/photos/missing/check", self._check)
        page.route("**/api/photos/missing", self._status)
        page.route("**/api/photos/missing?*", self._status)

    def _remove(self, route):
        self.remove_calls += 1
        if self.remove is None:
            self.held_removes.append(route)
        elif isinstance(self.remove, int):
            route.fulfill(status=self.remove, json={"error": "request failed"})
        else:
            route.fulfill(json=self.remove)

    def _check(self, route):
        self.check_calls += 1
        if self.check_status != 200:
            route.fulfill(status=self.check_status, json={"error": "refresh failed"})
        else:
            route.fulfill(json=self.check_payload)

    def _status(self, route):
        route.fulfill(json=self.status_payload)


def _open_browse_with_two_missing_selected(live_server, page):
    server = MissingOriginalsServer(page)
    page.goto(f"{live_server['url']}/browse")
    record_toasts(page)
    page.evaluate(
        """() => {
          const list = document.getElementById('missingPhotosList');
          list.innerHTML = `
            <div class="missing-photo-row" data-photo-id="101">
              <input type="checkbox" checked>
              <button type="button">Remove</button>
            </div>
            <div class="missing-photo-row" data-photo-id="102">
              <input type="checkbox" checked>
              <button type="button">Remove</button>
            </div>`;
          document.getElementById('missingPhotosToolbar').style.display = 'flex';
          toggleMissingPhoto(101, true);
          toggleMissingPhoto(102, true);
          window.confirm = () => true;
          Vireo.registerPageHooks({loadMisses: async () => {}});
        }"""
    )
    return server


def _remove_selected(page):
    page.evaluate(
        """() => {
          window.__missingRemoveFinished = false;
          removeSelectedMissingPhotos().then(() => {
            window.__missingRemoveFinished = true;
          });
        }"""
    )


def test_missing_originals_delete_shows_busy_and_success_feedback(live_server, page):
    server = _open_browse_with_two_missing_selected(live_server, page)
    server.remove = None
    _remove_selected(page)

    remove_button = page.locator("#missingPhotosRemoveBtn")
    expect(remove_button).to_be_disabled()
    expect(remove_button).to_have_text("Removing 2 photos…")
    expect(page.locator("#missingPhotosRefreshBtn")).to_be_disabled()
    expect(page.locator("#missingPhotosList input").first).to_be_disabled()
    expect(page.locator("#missingPhotosActionStatus")).to_have_text(
        "Removing 2 photos…"
    )

    # A second click while the request is pending must not submit twice.
    page.evaluate("removeSelectedMissingPhotos()")
    page.wait_for_timeout(100)
    assert server.remove_calls == 1

    server.held_removes[0].fulfill(json=REMOVED)
    page.wait_for_function("window.__missingRemoveFinished === true")

    expect(remove_button).to_have_text("Remove selected")
    expect(page.locator("#missingPhotosRefreshBtn")).to_be_enabled()
    expect(page.locator("#missingPhotosActionStatus")).to_have_text(
        "Removed 2 photos."
    )
    # The refresh re-read the list: nothing is missing, so nothing is
    # selected and Remove stays disabled until something is.
    expect(page.locator("#missingPhotosList")).to_have_text("No photos with missing originals.")
    expect(remove_button).to_be_disabled()
    assert server.check_calls == 1
    assert toasts(page, "Removed") == [
        {"message": "Removed 2 photos from Vireo.", "type": "success"}
    ]


def test_missing_originals_delete_failure_restores_controls(live_server, page):
    server = _open_browse_with_two_missing_selected(live_server, page)
    server.remove = 500
    _remove_selected(page)
    page.wait_for_function("window.__missingRemoveFinished === true")

    expect(page.locator("#missingPhotosRemoveBtn")).to_be_enabled()
    expect(page.locator("#missingPhotosRefreshBtn")).to_be_enabled()
    expect(page.locator("#missingPhotosList input").first).to_be_enabled()
    status = page.locator("#missingPhotosActionStatus")
    expect(status).to_have_text(
        "Could not remove the selected photos. Please try again."
    )
    expect(status).to_have_attribute("role", "alert")
    assert server.check_calls == 0


def test_missing_originals_refresh_failure_preserves_delete_success(live_server, page):
    server = _open_browse_with_two_missing_selected(live_server, page)
    server.check_status = 500
    _remove_selected(page)
    page.wait_for_function("window.__missingRemoveFinished === true")

    status = page.locator("#missingPhotosActionStatus")
    expect(status).to_have_text(
        "Removed 2 photos, but could not refresh missing originals."
    )
    expect(status).to_have_attribute("role", "alert")
    expect(page.locator("#missingPhotosRemoveBtn")).to_be_enabled()
    assert server.check_calls == 1
    assert toasts(page, "Removed") == [
        {"message": "Removed 2 photos from Vireo.", "type": "success"}
    ]


def test_pending_missing_originals_refresh_finalizes_on_later_failure(
    live_server, page
):
    server = _open_browse_with_two_missing_selected(live_server, page)
    server.check_payload = {"status": "pending", "pending": True}
    server.status_payload = {"status": "pending", "pending": True}
    _remove_selected(page)
    page.wait_for_function("window.__missingRemoveFinished === true")

    status = page.locator("#missingPhotosActionStatus")
    expect(status).to_have_text(
        "Removed 2 photos. Rechecking missing originals…"
    )

    # The recheck then ends in an error; the next status read reports it.
    server.status_payload = {"status": "error", "error": "scan failed"}
    page.evaluate(
        """() => {
          window.__missingTerminalLoadFinished = false;
          loadMissingPhotos().then(() => {
            window.__missingTerminalLoadFinished = true;
          });
        }"""
    )
    page.wait_for_function("window.__missingTerminalLoadFinished === true")

    expect(status).to_have_text(
        "Removed 2 photos, but could not refresh missing originals."
    )
    expect(status).to_have_attribute("role", "alert")
