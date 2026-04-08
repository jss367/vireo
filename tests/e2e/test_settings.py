from playwright.sync_api import expect


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

    # Python version should be populated (not the initial placeholder)
    python_version = page.locator("#pythonVersion")
    expect(python_version).not_to_have_text("-", timeout=api_timeout)
    expect(python_version).to_contain_text("3.")

    # Compute device should be populated
    device_name = page.locator("#deviceName")
    expect(device_name).not_to_have_text("-", timeout=api_timeout)
