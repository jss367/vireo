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


def test_import_source_browse_button_shows_quick_photo_count(live_server, page):
    url = live_server["url"]
    page.goto(f"{url}/import")
    page.evaluate(
        """
        () => {
          const originalFetch = window.fetch.bind(window);
          window.fetch = (input, init) => {
            const target = typeof input === 'string' ? input : input.url;
            if (target && target.indexOf('/api/import/folder-preview') === 0) {
              return Promise.resolve(new Response(JSON.stringify({
                total_count: 42,
                total_size: 0,
                type_breakdown: {'.jpg': 42},
                duplicate_count: 0,
                files: [],
              }), {status: 200, headers: {'Content-Type': 'application/json'}}));
            }
            return originalFetch(input, init);
          };
          window.pickDirectory = async () => ['/tmp/card-a'];
        }
        """
    )

    page.locator("[data-testid='import-source-browse-btn']").click()

    source_list = page.locator("#sourceList")
    expect(source_list).to_contain_text("/tmp/card-a")
    expect(source_list.locator(".source-meta")).to_have_text("42 photos")


def test_import_destination_browse_button_sets_destination(live_server, page):
    url = live_server["url"]
    page.goto(f"{url}/import")
    page.evaluate("window.pickDirectory = async () => '/tmp/archive'")
    page.locator("#modeCopy").check()

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
    expect(page.locator("#folderBrowserTitle")).to_have_text("Select Source Folders")
    expect(page.locator(".folder-browser-panel")).to_have_attribute("role", "dialog")
    expect(page.locator(".folder-browser-panel")).to_have_attribute("aria-modal", "true")
    expect(page.locator(".folder-browser-panel")).to_have_attribute(
        "aria-labelledby", "folderBrowserTitle")


def test_import_folder_browser_selects_multiple_source_folders(live_server, page):
    url = live_server["url"]
    page.goto(f"{url}/import")
    page.evaluate("window.pickDirectory = async () => null")
    page.evaluate(
        """
        () => {
          const originalFetch = window.fetch.bind(window);
          window.fetch = (input, init) => {
            const target = typeof input === 'string' ? input : input.url;
            if (target && target.indexOf('/api/browse') === 0) {
              return Promise.resolve(new Response(JSON.stringify({
                path: '/tmp',
                dirs: [
                  {name: 'card-a', path: '/tmp/card-a'},
                  {name: 'card-b', path: '/tmp/card-b'},
                  {name: 'card-c', path: '/tmp/card-c'},
                ],
              }), {status: 200, headers: {'Content-Type': 'application/json'}}));
            }
            return originalFetch(input, init);
          };
        }
        """
    )

    page.locator("[data-testid='import-source-browse-btn']").click()
    items = page.locator("#folderBrowserList .folder-browser-item[data-folder-path]")
    expect(items).to_have_count(3)

    items.nth(0).click()
    items.nth(2).click(modifiers=["Shift"])

    expect(page.locator("#folderBrowserSelectBtn")).to_have_text("Add 3 Folders")
    page.locator("#folderBrowserSelectBtn").click()

    source_list = page.locator("#sourceList")
    expect(source_list).to_contain_text("/tmp/card-a")
    expect(source_list).to_contain_text("/tmp/card-b")
    expect(source_list).to_contain_text("/tmp/card-c")


def test_import_folder_browser_toggles_discontiguous_source_folders(live_server, page):
    url = live_server["url"]
    page.goto(f"{url}/import")
    page.evaluate("window.pickDirectory = async () => null")
    page.evaluate(
        """
        () => {
          const originalFetch = window.fetch.bind(window);
          window.fetch = (input, init) => {
            const target = typeof input === 'string' ? input : input.url;
            if (target && target.indexOf('/api/browse') === 0) {
              return Promise.resolve(new Response(JSON.stringify({
                path: '/tmp',
                dirs: [
                  {name: 'card-a', path: '/tmp/card-a'},
                  {name: 'card-b', path: '/tmp/card-b'},
                  {name: 'card-c', path: '/tmp/card-c'},
                ],
              }), {status: 200, headers: {'Content-Type': 'application/json'}}));
            }
            return originalFetch(input, init);
          };
        }
        """
    )

    page.locator("[data-testid='import-source-browse-btn']").click()
    items = page.locator("#folderBrowserList .folder-browser-item[data-folder-path]")
    expect(items).to_have_count(3)

    items.nth(0).click()
    items.nth(2).evaluate(
        """el => el.dispatchEvent(new MouseEvent('click', {
          bubbles: true,
          ctrlKey: true,
        }))"""
    )

    expect(page.locator("#folderBrowserSelectBtn")).to_have_text("Add 2 Folders")
    page.locator("#folderBrowserSelectBtn").click()

    source_list = page.locator("#sourceList")
    expect(source_list).to_contain_text("/tmp/card-a")
    expect(source_list).not_to_contain_text("/tmp/card-b")
    expect(source_list).to_contain_text("/tmp/card-c")


def test_import_folder_browser_selects_volumes_from_synthetic_root(live_server, page):
    # The Volumes shortcut renders /api/volumes as a synthetic root with
    # browserPath = ''. Volume rows are selectable, so the Add button must
    # enable once at least one is picked and must submit the selected drives
    # instead of the (empty) synthetic root.
    url = live_server["url"]
    page.goto(f"{url}/import")
    page.evaluate("window.pickDirectory = async () => null")
    page.evaluate(
        """
        () => {
          Object.defineProperty(navigator, 'userAgent', {
            value: 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 Chrome/120 Safari/537.36',
            configurable: true,
          });
          const originalFetch = window.fetch.bind(window);
          window.fetch = (input, init) => {
            const target = typeof input === 'string' ? input : input.url;
            if (target && target.indexOf('/api/volumes') === 0) {
              return Promise.resolve(new Response(JSON.stringify([
                {name: 'Volume A', path: '/Volumes/A'},
                {name: 'Volume B', path: '/Volumes/B'},
              ]), {status: 200, headers: {'Content-Type': 'application/json'}}));
            }
            return originalFetch(input, init);
          };
        }
        """
    )

    page.locator("[data-testid='import-source-browse-btn']").click()
    # Non-Mac branch fetches /api/volumes (stubbed) and renders the drives
    # as selectable rows in a synthetic root with browserPath = ''.
    page.evaluate("async () => { await browseImportFolderTo('__volumes__'); }")

    items = page.locator("#folderBrowserList .folder-browser-item[data-folder-path]")
    expect(items).to_have_count(2)

    select_btn = page.locator("#folderBrowserSelectBtn")
    expect(select_btn).to_be_disabled()

    items.nth(0).evaluate(
        """el => el.dispatchEvent(new MouseEvent('click', {
          bubbles: true,
          ctrlKey: true,
        }))"""
    )
    items.nth(1).evaluate(
        """el => el.dispatchEvent(new MouseEvent('click', {
          bubbles: true,
          ctrlKey: true,
        }))"""
    )

    expect(select_btn).to_be_enabled()
    expect(select_btn).to_have_text("Add 2 Folders")
    select_btn.click()

    source_list = page.locator("#sourceList")
    expect(source_list).to_contain_text("/Volumes/A")
    expect(source_list).to_contain_text("/Volumes/B")


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


def test_use_staging_as_import_source_forces_copy_mode(live_server, page):
    # Orphaned-staging recovery must copy the staging tree into the archive.
    # In-place would catalog paths that vanish when staging is cleaned up,
    # so useStagingAsImportSource() has to flip the mode to Copy regardless
    # of the page default.
    url = live_server["url"]
    page.goto(f"{url}/import")

    # Default is in_place — sanity check before invoking the recovery helper.
    expect(page.locator("#modeInPlace")).to_be_checked()

    page.evaluate(
        """
        () => useStagingAsImportSource({
          source_root: '/tmp/staging-src',
          inferred_destination: '/tmp/archive-dest',
        })
        """
    )

    expect(page.locator("#modeCopy")).to_be_checked()
    expect(page.locator("#modeInPlace")).not_to_be_checked()
    expect(page.locator("#destInput")).to_have_value("/tmp/archive-dest")
    expect(page.locator("#sourceList")).to_contain_text("/tmp/staging-src")
    # updateImportMode() must have run so the destination card is visible again.
    expect(page.locator("#destCard")).to_be_visible()


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
