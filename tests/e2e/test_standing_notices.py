"""Standing notices sit behind the navbar's bell instead of in rows above the
page: the bell counts them, and the panel lists them with their actions."""

import pytest
from playwright.sync_api import expect

from e2e.notices import open_notices


def _pending_sync(route):
    route.fulfill(json={"pending_count": 3, "pending_photo_count": 2, "active_job": None})


def _duplicate_copies(route):
    route.fulfill(json={"count": 4, "total_size": 9_600_000})


def test_a_page_without_notices_has_a_quiet_bell(live_server, page):
    page.goto(f"{live_server['url']}/browse")
    page.locator(".grid-card").first.wait_for(state="visible")
    expect(page.locator("#notificationBadge")).to_be_hidden()
    open_notices(page)
    expect(page.locator("#standingNoticesEmpty")).to_have_text("Nothing needs attention.")
    expect(page.locator("#notificationList")).to_have_text("No notifications yet.")


def test_sync_start_error_is_visible_with_the_notice_panel_closed(live_server, page):
    """A failed request must be visible when the standing status row is hidden."""
    page.route("**/api/sync/status", _pending_sync)
    page.route("**/api/jobs/sync", lambda route: route.fulfill(
        status=503, json={"error": "Synthetic sync unavailable"},
    ))
    page.goto(f"{live_server['url']}/browse")
    expect(page.locator("#notificationBadge")).to_have_text("1")
    expect(page.locator("#notificationHistory")).to_be_hidden()
    page.evaluate("void runSync()")
    expect(page.locator("#syncStatus")).to_contain_text("Synthetic sync unavailable")
    expect(page.locator('.notification-toast[data-type="error"]')).to_contain_text("Synthetic sync unavailable")


@pytest.mark.parametrize('banner, overlay', [
    ('missingFoldersBanner', 'missingFoldersModal'),
    ('missingPhotosBanner', 'missingPhotosModal'),
    ('syncBanner', 'syncPreviewOverlay'),
])
def test_notice_review_transfers_keyboard_focus(live_server, page, banner, overlay):
    page.route('**/api/sync/status', _pending_sync)
    page.goto(f"{live_server['url']}/browse")
    page.locator('.grid-card').first.wait_for(state='visible')
    page.evaluate("id => { document.getElementById(id).style.display = 'flex'; }", banner)
    open_notices(page)
    action = page.locator(f'#{banner} [data-notice-closes-panel]')
    action.focus()
    page.keyboard.press('Enter')
    expect(page.locator('#' + overlay)).to_be_visible()
    expect(page.locator('#notificationHistory')).to_be_hidden()
    assert page.evaluate('id => document.getElementById(id).contains(document.activeElement)', overlay)


def test_pending_import_keeps_its_progress_visible(live_server, page):
    held = []
    page.route('**/api/workspaces/active/new-images/snapshot', lambda route: held.append(route))
    page.goto(f"{live_server['url']}/browse")
    page.locator('.grid-card').first.wait_for(state='visible')
    page.evaluate("() => { document.getElementById('newImagesBanner').style.display = 'flex'; }")
    open_notices(page)
    button = page.locator('#newImagesBanner .banner-cta')
    try:
        button.click()
        expect(button).to_have_text('Opening...')
        expect(button).to_be_visible()
    finally:
        for route in held:
            route.abort()


def test_notices_are_counted_on_the_bell_and_take_no_row_above_the_photos(live_server, page):
    page.goto(f"{live_server['url']}/browse")
    first_card = page.locator(".grid-card").first
    first_card.wait_for(state="visible")
    top_without_notices = first_card.bounding_box()["y"]

    page.route("**/api/sync/status", _pending_sync)
    page.route("**/api/duplicates/disk-cleanup-summary", _duplicate_copies)
    page.goto(f"{live_server['url']}/browse")
    badge = page.locator("#notificationBadge")
    expect(badge).to_have_text("2")
    first_card.wait_for(state="visible")
    assert first_card.bounding_box()["y"] == top_without_notices

    # The page's own notice (pending XMP changes) joins the navbar's.
    assert page.evaluate("document.getElementById('syncBanner').parentElement.id") == "standingNotices"
    expect(page.locator("#syncBanner")).to_be_hidden()
    bell = page.locator("#notificationsToggle")
    expect(bell).to_have_attribute("aria-label", "Notifications, 2 need attention")
    title = bell.get_attribute("title")
    assert "4 duplicate file copies could be cleaned up" in title
    assert "3 changes across 2 photos" in title

    open_notices(page)
    expect(page.locator("#standingNoticesEmpty")).to_be_hidden()
    expect(page.locator("#dupCleanupBanner")).to_be_visible()
    expect(page.locator("#syncBannerText")).to_contain_text("3 changes across 2 photos")

    page.locator("#dupCleanupBanner .banner-dismiss").click()
    expect(page.locator("#dupCleanupBanner")).to_be_hidden()
    expect(badge).to_have_text("1")
    expect(page.locator("#notificationHistory")).to_be_visible()

    # An action that opens a dialog gets out of the dialog's way.
    page.locator("#syncBannerReviewButton").click()
    expect(page.locator("#syncPreviewOverlay")).to_be_visible()
    expect(page.locator("#notificationHistory")).to_be_hidden()
    expect(badge).to_have_text("1")


# A desktop shell with a downloaded update: update_status reports its version,
# and the first restart_to_update fails as a failed install does.
_FAKE_SHELL_WITH_READY_UPDATE = """
window.__vireoInvoked = [];
window.__TAURI_INTERNALS__ = {
  invoke(command) {
    window.__vireoInvoked.push(command);
    if (command === 'update_status') {
      return Promise.resolve({version: '0.99.0', waiting_for: null, restart_in: null});
    }
    if (command === 'restart_to_update') return Promise.reject('Synthetic install failure');
    return Promise.resolve(null);
  },
};
"""


def test_a_downloaded_update_waits_under_the_bell(live_server, page):
    """A ready update badges the bell instead of opening a dialog, and a failed
    install keeps it ready with the reason."""
    page.add_init_script(_FAKE_SHELL_WITH_READY_UPDATE)
    page.goto(f"{live_server['url']}/browse")
    page.locator(".grid-card").first.wait_for(state="visible")
    expect(page.locator("#notificationBadge")).to_have_text("1")
    open_notices(page)
    notice = page.locator("#updateReadyNotice")
    expect(notice).to_contain_text("Vireo v0.99.0 has been downloaded.")
    notice.get_by_role("button", name="Restart to Update").click()
    expect(notice).to_contain_text("The last attempt failed: Synthetic install failure")
    expect(notice.get_by_role("button", name="Restart to Update")).to_be_enabled()
    assert "restart_to_update" in page.evaluate("window.__vireoInvoked")


# A desktop shell waiting for an import before it restarts. A status event from
# the shell starts the countdown; Not Now answers it and the countdown ends.
_FAKE_SHELL_WAITING_FOR_JOBS = """
window.__vireoInvoked = [];
window.__updateStatus = {version: '0.99.0', waiting_for: 'Import', restart_in: null};
window.__TAURI_INTERNALS__ = {
  invoke(command, args) {
    window.__vireoInvoked.push([command, args || null]);
    if (command === 'update_status') return Promise.resolve(window.__updateStatus);
    if (command === 'respond_update_restart') {
      window.__updateStatus = {version: '0.99.0', waiting_for: null, restart_in: null};
      window.dispatchEvent(new Event('vireo:update-status'));
    }
    return Promise.resolve(null);
  },
};
"""


def test_a_restart_waiting_for_jobs_names_them_and_counts_down(live_server, page):
    """While a restart waits for the user's jobs, the notice names them; when
    they finish, a banner counts down and Not Now keeps the update ready."""
    page.add_init_script(_FAKE_SHELL_WAITING_FOR_JOBS)
    page.goto(f"{live_server['url']}/browse")
    page.locator(".grid-card").first.wait_for(state="visible")
    open_notices(page)
    notice = page.locator("#updateReadyNotice")
    expect(notice).to_contain_text("Vireo will restart to install it after Import finishes.")
    expect(notice.get_by_role("button", name="Restart Now")).to_be_visible()
    expect(page.locator("#updateRestartBanner")).to_have_count(0)

    page.evaluate("""() => {
      window.__updateStatus = {version: '0.99.0', waiting_for: null, restart_in: 29};
      window.dispatchEvent(new Event('vireo:update-status'));
    }""")
    banner = page.locator("#updateRestartBanner")
    expect(banner).to_contain_text("Vireo will restart to install v0.99.0 in")
    banner.get_by_role("button", name="Not Now").click()
    expect(banner).to_have_count(0)
    expect(notice).to_contain_text("Restart Vireo to finish updating.")
    assert ["respond_update_restart", {"action": "cancel"}] in page.evaluate(
        "window.__vireoInvoked"
    )


def test_no_update_notice_without_a_desktop_shell(live_server, page):
    page.goto(f"{live_server['url']}/browse")
    page.locator(".grid-card").first.wait_for(state="visible")
    expect(page.locator("#updateReadyNotice")).to_be_hidden()
