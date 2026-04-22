import re

from playwright.sync_api import expect


def test_open_context_menu_at_cursor(live_server, page):
    """openContextMenu() places the menu near the event coords and renders items."""
    url = live_server["url"]
    page.goto(f"{url}/browse")

    page.evaluate("""
        window.__ctx_hit = null;
        openContextMenu({clientX: 200, clientY: 150}, [
            {label: 'Alpha', onClick: () => window.__ctx_hit = 'alpha'},
            {separator: true},
            {label: 'Beta', disabled: true, disabledHint: 'nope'},
        ]);
    """)

    menu = page.locator(".vireo-ctx-menu")
    expect(menu).to_be_visible()
    expect(menu.locator(".vireo-ctx-item", has_text="Alpha")).to_be_visible()
    beta = menu.locator(".vireo-ctx-item", has_text="Beta")
    expect(beta).to_have_class(re.compile(r"vireo-ctx-disabled"))

    # Click Alpha; menu closes and handler fires.
    menu.locator(".vireo-ctx-item", has_text="Alpha").click()
    expect(menu).to_be_hidden()
    assert page.evaluate("window.__ctx_hit") == "alpha"


def test_context_menu_dismiss_outside_click(live_server, page):
    url = live_server["url"]
    page.goto(f"{url}/browse")

    page.evaluate("""
        openContextMenu({clientX: 100, clientY: 100},
            [{label: 'X', onClick: () => {}}]);
    """)
    expect(page.locator(".vireo-ctx-menu")).to_be_visible()

    page.mouse.click(500, 500)
    expect(page.locator(".vireo-ctx-menu")).to_be_hidden()


def test_context_menu_escape_closes(live_server, page):
    url = live_server["url"]
    page.goto(f"{url}/browse")

    page.evaluate("""
        openContextMenu({clientX: 50, clientY: 50},
            [{label: 'Y', onClick: () => {}}]);
    """)
    page.keyboard.press("Escape")
    expect(page.locator(".vireo-ctx-menu")).to_be_hidden()
