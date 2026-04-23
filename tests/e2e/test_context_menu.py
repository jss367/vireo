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


def test_escape_does_not_propagate_to_page(live_server, page):
    """Escape dismissing the menu must not reach page-level Escape handlers.

    Regression guard: before the stopPropagation fix, Escape while a menu
    was open would also fire browse's clearSelection() (resetting
    selectedPhotos / selectedIndex) and closeDetail(). The Shift-click anchor
    test relies on the fix and is the primary consumer, but this unit
    captures the behavior independently so the shared component owns it.
    """
    url = live_server["url"]
    page.goto(f"{url}/browse")

    page.evaluate(
        """() => {
            window.__esc_leaked = 0;
            document.body.addEventListener('keydown', (e) => {
                if (e.key === 'Escape') window.__esc_leaked++;
            }, false);
            openContextMenu({clientX: 40, clientY: 40},
                [{label: 'Z', onClick: () => {}}]);
        }"""
    )
    expect(page.locator(".vireo-ctx-menu")).to_be_visible()
    page.keyboard.press("Escape")
    expect(page.locator(".vireo-ctx-menu")).to_be_hidden()
    assert page.evaluate("window.__esc_leaked") == 0


def test_context_menu_chip_row_renders_and_fires(live_server, page):
    url = live_server["url"]
    page.goto(f"{url}/browse")
    page.evaluate("""() => {
        window.__ctx_chip = null;
        openContextMenu({clientX: 100, clientY: 100}, [
            { chips: [
                {label: 'A', onClick: () => window.__ctx_chip = 'a'},
                {label: 'B', onClick: () => window.__ctx_chip = 'b'},
            ] },
        ]);
    }""")
    from playwright.sync_api import expect
    menu = page.locator(".vireo-ctx-menu")
    expect(menu).to_be_visible()
    chips = menu.locator(".vireo-ctx-chip")
    assert chips.count() == 2
    chips.nth(1).click()
    expect(menu).to_be_hidden()
    assert page.evaluate("window.__ctx_chip") == "b"


def test_coerce_selection_inside_keeps_set(live_server, page):
    url = live_server["url"]
    page.goto(f"{url}/browse")
    out = page.evaluate("""() => {
        const sel = new Set([1, 2, 3]);
        const result = coerceSelectionOnContext(sel, 2);
        return { size: sel.size, has1: sel.has(1), has2: sel.has(2), has3: sel.has(3), result: Array.from(result) };
    }""")
    assert out["size"] == 3
    assert out["has1"] and out["has2"] and out["has3"]
    assert sorted(out["result"]) == [1, 2, 3]


def test_coerce_selection_outside_replaces(live_server, page):
    url = live_server["url"]
    page.goto(f"{url}/browse")
    out = page.evaluate("""() => {
        const sel = new Set([1, 2, 3]);
        const result = coerceSelectionOnContext(sel, 99);
        return { size: sel.size, has99: sel.has(99), result: Array.from(result) };
    }""")
    assert out["size"] == 1
    assert out["has99"] is True
    assert out["result"] == [99]


def test_coerce_selection_string_ids(live_server, page):
    url = live_server["url"]
    page.goto(f"{url}/browse")
    out = page.evaluate("""() => {
        const sel = new Set(["1", "2", "3"]);
        const result = coerceSelectionOnContext(sel, "2");
        return { size: sel.size, result: Array.from(result).sort() };
    }""")
    assert out["size"] == 3
    assert out["result"] == ["1", "2", "3"]


def test_coerce_selection_null_id_noop(live_server, page):
    url = live_server["url"]
    page.goto(f"{url}/browse")
    out = page.evaluate("""() => {
        const sel = new Set(["a", "b"]);
        const result = coerceSelectionOnContext(sel, null);
        return { size: sel.size, result: Array.from(result).sort() };
    }""")
    assert out["size"] == 2
    assert out["result"] == ["a", "b"]
