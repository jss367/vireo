"""Standing notices (new images, missing folders, duplicate cleanup, pending
XMP changes) are rows in the notifications panel behind the navbar's bell."""

from playwright.sync_api import expect


def open_notices(page):
    """Open the notifications panel so its notice rows can be read and used."""
    panel = page.locator("#notificationHistory")
    if not panel.is_visible():
        page.locator("#notificationsToggle").click()
    expect(panel).to_be_visible()
