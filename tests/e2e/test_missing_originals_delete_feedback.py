from playwright.sync_api import expect


def _prepare_missing_originals_delete(page):
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
          window.__missingRemoveToasts = [];
          window.showToast = (message, kind) => {
            window.__missingRemoveToasts.push({message, kind});
          };
          window.checkMissingPhotos = () => {};
          Vireo.registerPageHooks({loadMisses: async () => {}});
          window.__missingRefreshCount = 0;
          window.refreshMissingPhotosNow = async () => {
            window.__missingRefreshCount += 1;
            return 'ready';
          };
        }"""
    )


def test_missing_originals_delete_shows_busy_and_success_feedback(live_server, page):
    page.goto(f"{live_server['url']}/browse")
    _prepare_missing_originals_delete(page)

    page.evaluate(
        """() => {
          // safeFetch is the page-wide fetch helper, so only count calls to
          // the remove endpoint. Unrelated page code (navbar refreshes,
          // photo loads) also goes through safeFetch and would otherwise
          // inflate the count under a slow full-suite run.
          const realSafeFetch = window.safeFetch;
          window.__missingRemoveCallCount = 0;
          window.safeFetch = (url, ...rest) => {
            if (!String(url).startsWith('/api/photos/missing/remove')) {
              return realSafeFetch(url, ...rest);
            }
            window.__missingRemoveCallCount += 1;
            return new Promise(resolve => {
              window.__resolveMissingRemove = resolve;
            });
          };
          window.__missingRemoveFinished = false;
          removeSelectedMissingPhotos().then(() => {
            window.__missingRemoveFinished = true;
          });
        }"""
    )

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
    assert page.evaluate("window.__missingRemoveCallCount") == 1

    page.evaluate(
        """() => window.__resolveMissingRemove({
          deleted: 2,
          restored: [],
          folder_offline: [],
          skipped: 0,
        })"""
    )
    page.wait_for_function("window.__missingRemoveFinished === true")

    expect(remove_button).to_have_text("Remove selected")
    expect(remove_button).to_be_enabled()
    expect(page.locator("#missingPhotosRefreshBtn")).to_be_enabled()
    expect(page.locator("#missingPhotosActionStatus")).to_have_text(
        "Removed 2 photos."
    )
    assert page.evaluate("window.__missingRefreshCount") == 1
    assert page.evaluate("window.__missingRemoveToasts") == [
        {"message": "Removed 2 photos from Vireo.", "kind": "success"}
    ]


def test_missing_originals_delete_failure_restores_controls(live_server, page):
    page.goto(f"{live_server['url']}/browse")
    _prepare_missing_originals_delete(page)

    page.evaluate(
        """() => {
          window.safeFetch = async () => {
            throw new Error('request failed');
          };
          window.__missingRemoveFinished = false;
          removeSelectedMissingPhotos().then(() => {
            window.__missingRemoveFinished = true;
          });
        }"""
    )
    page.wait_for_function("window.__missingRemoveFinished === true")

    expect(page.locator("#missingPhotosRemoveBtn")).to_be_enabled()
    expect(page.locator("#missingPhotosRefreshBtn")).to_be_enabled()
    expect(page.locator("#missingPhotosList input").first).to_be_enabled()
    status = page.locator("#missingPhotosActionStatus")
    expect(status).to_have_text(
        "Could not remove the selected photos. Please try again."
    )
    expect(status).to_have_attribute("role", "alert")
    assert page.evaluate("window.__missingRefreshCount") == 0


def test_missing_originals_refresh_failure_preserves_delete_success(live_server, page):
    page.goto(f"{live_server['url']}/browse")
    _prepare_missing_originals_delete(page)

    page.evaluate(
        """() => {
          window.safeFetch = async () => ({
            deleted: 2,
            restored: [],
            folder_offline: [],
            skipped: 0,
          });
          window.refreshMissingPhotosNow = async () => {
            window.__missingRefreshCount += 1;
            throw new Error('refresh failed');
          };
          window.__missingRemoveFinished = false;
          removeSelectedMissingPhotos().then(() => {
            window.__missingRemoveFinished = true;
          });
        }"""
    )
    page.wait_for_function("window.__missingRemoveFinished === true")

    status = page.locator("#missingPhotosActionStatus")
    expect(status).to_have_text(
        "Removed 2 photos, but could not refresh missing originals."
    )
    expect(status).to_have_attribute("role", "alert")
    expect(page.locator("#missingPhotosRemoveBtn")).to_be_enabled()
    assert page.evaluate("window.__missingRefreshCount") == 1
    assert page.evaluate("window.__missingRemoveToasts") == [
        {"message": "Removed 2 photos from Vireo.", "kind": "success"}
    ]


def test_pending_missing_originals_refresh_finalizes_on_later_failure(
    live_server, page
):
    page.goto(f"{live_server['url']}/browse")
    _prepare_missing_originals_delete(page)

    page.evaluate(
        """() => {
          window.safeFetch = async () => ({
            deleted: 2,
            restored: [],
            folder_offline: [],
            skipped: 0,
          });
          window.refreshMissingPhotosNow = async () => {
            window.__missingRefreshCount += 1;
            return 'pending';
          };
          window.__missingRemoveFinished = false;
          removeSelectedMissingPhotos().then(() => {
            window.__missingRemoveFinished = true;
          });
        }"""
    )
    page.wait_for_function("window.__missingRemoveFinished === true")

    status = page.locator("#missingPhotosActionStatus")
    expect(status).to_have_text(
        "Removed 2 photos. Rechecking missing originals…"
    )

    page.evaluate(
        """() => {
          window.fetch = async () => ({
            ok: true,
            json: async () => ({status: 'error', error: 'scan failed'}),
          });
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
