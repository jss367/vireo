import re

from playwright.sync_api import expect


def test_import_source_browse_button_adds_source_folder(live_server, page):
    url = live_server["url"]
    page.goto(f"{url}/import")
    page.evaluate("window.pickDirectory = async () => ['/tmp/card-a', '/tmp/card-b']")

    browse_btn = page.locator("[data-testid='import-source-browse-btn']")
    expect(browse_btn).to_be_visible()
    browse_btn.click()

    source_list = page.locator("#sourceList")
    expect(source_list).to_contain_text("/tmp/card-a")
    expect(source_list).to_contain_text("/tmp/card-b")


def test_import_destination_browse_button_sets_destination(live_server, page):
    url = live_server["url"]
    page.goto(f"{url}/import")
    page.evaluate("window.pickDirectory = async () => '/tmp/archive'")

    browse_btn = page.locator("[data-testid='import-destination-browse-btn']")
    expect(browse_btn).to_be_visible()
    browse_btn.click()

    expect(page.locator("#destInput")).to_have_value("/tmp/archive")


def test_import_browse_button_opens_folder_browser_fallback(live_server, page):
    url = live_server["url"]
    page.goto(f"{url}/import")
    page.evaluate("window.pickDirectory = async () => null")

    page.locator("[data-testid='import-source-browse-btn']").click()

    browser = page.locator("[data-testid='import-folder-browser']")
    expect(browser).to_have_class(re.compile(r"\bopen\b"))
    expect(page.locator("#folderBrowserTitle")).to_have_text("Select Source Folder")
    expect(page.locator(".folder-browser-panel")).to_have_attribute("role", "dialog")
    expect(page.locator(".folder-browser-panel")).to_have_attribute("aria-modal", "true")
    expect(page.locator(".folder-browser-panel")).to_have_attribute(
        "aria-labelledby", "folderBrowserTitle")


def test_import_folder_browser_disables_select_while_pending(live_server, page):
    # A stale browserPath from a prior fetch used to remain selectable during
    # the next navigation. If the fetch stalled or failed, clicking "Select
    # This Folder" would submit the previous folder. Guard: the button must
    # be disabled while /api/browse is in flight and only re-enable once a
    # real path resolves.
    url = live_server["url"]
    page.goto(f"{url}/import")
    page.evaluate("window.pickDirectory = async () => null")
    page.evaluate(
        """
        () => {
          const originalFetch = window.fetch.bind(window);
          window.__releaseBrowse = null;
          window.fetch = (input, init) => {
            const target = typeof input === 'string' ? input : input.url;
            if (target && target.indexOf('/api/browse') === 0) {
              return new Promise((resolve) => {
                window.__releaseBrowse = () => resolve(new Response(
                  JSON.stringify({path: '/tmp/target', dirs: []}),
                  {status: 200, headers: {'Content-Type': 'application/json'}}
                ));
              });
            }
            return originalFetch(input, init);
          };
        }
        """
    )

    page.locator("[data-testid='import-source-browse-btn']").click()

    select_btn = page.locator("#folderBrowserSelectBtn")
    expect(select_btn).to_be_disabled()

    page.evaluate("() => window.__releaseBrowse && window.__releaseBrowse()")
    expect(select_btn).to_be_enabled()


def test_import_folder_browser_escape_closes_modal(live_server, page):
    url = live_server["url"]
    page.goto(f"{url}/import")
    page.evaluate("window.pickDirectory = async () => null")

    page.locator("[data-testid='import-source-browse-btn']").click()
    expect(page.locator("[data-testid='import-folder-browser']")).to_have_class(
        re.compile(r"\bopen\b"))
    expect(page.locator(".folder-browser-close")).to_be_focused()

    page.keyboard.press("Escape")

    expect(page.locator("[data-testid='import-folder-browser']")).not_to_have_class(
        re.compile(r"\bopen\b"))
