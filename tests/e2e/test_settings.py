from playwright.sync_api import expect


def test_settings_links_to_storage_page(live_server, page):
    """Settings makes the dedicated storage page discoverable."""
    url = live_server["url"]
    page.goto(f"{url}/settings", timeout=5000)

    storage_link = page.get_by_role("link", name="Open Storage")
    expect(storage_link).to_be_visible()
    expect(storage_link).to_have_attribute("href", "/storage")


def test_settings_system_info_renders(live_server, page):
    """Settings page loads and displays system info section with real data."""
    url = live_server["url"]
    page.goto(f"{url}/settings", timeout=5000)

    # The system info section should be visible
    system_section = page.locator(".section-title", has_text="System")
    expect(system_section).to_be_visible()

    # /api/system/info populates these fields asynchronously via JS.
    # Allow a generous timeout for the async fetch to complete.
    api_timeout = 30_000

    # Compute device should be populated
    device_name = page.locator("#deviceName")
    expect(device_name).not_to_have_text("-", timeout=api_timeout)


def test_settings_cmd_f_opens_page_text_search(live_server, page):
    """Settings captures find shortcut and highlights page text matches."""
    url = live_server["url"]
    page.goto(f"{url}/settings", timeout=5000)

    page.keyboard.press("Control+F")

    find_panel = page.locator("#settingsFindPanel")
    find_input = page.locator("#settingsFindInput")
    expect(find_panel).to_be_visible()
    expect(find_input).to_be_focused()

    find_input.fill("api")
    expect(page.locator(".settings-find-mark").first).to_be_visible()
    expect(page.locator(".settings-find-mark.active")).to_have_count(1)
    expect(page.locator("#settingsFindStatus")).to_contain_text("of")


def test_settings_text_search_ignores_hidden_update_section(live_server, page):
    """Settings find should not count permanently hidden page text."""
    url = live_server["url"]
    page.goto(f"{url}/settings", timeout=5000)

    page.keyboard.press("Control+F")
    page.locator("#settingsFindInput").fill("Check for Updates")

    expect(page.locator(".settings-find-mark")).to_have_count(0)
    expect(page.locator("#settingsFindStatus")).to_have_text("0 results")
