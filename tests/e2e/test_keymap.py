"""End-to-end tests for the keymap registry and dispatcher."""



def test_keymap_globals_exposed(live_server, page):
    """Loading any page exposes the Keymap module on window."""
    url = live_server["url"]
    page.goto(f"{url}/browse", timeout=5000)
    page.wait_for_load_state("networkidle")

    # Module is loaded
    assert page.evaluate("typeof window.Keymap") == "object"

    # Helpers are exposed
    assert page.evaluate("typeof window.Keymap.parseShortcut") == "function"
    assert page.evaluate("typeof window.Keymap.matchesShortcut") == "function"
    assert page.evaluate("typeof window.Keymap.isInputFocused") == "function"


def test_keymap_register_and_lookup(live_server, page):
    """register() stores shortcuts; shortcutsForScope() returns them merged with global."""
    url = live_server["url"]
    page.goto(f"{url}/browse", timeout=5000)
    page.wait_for_load_state("networkidle")

    page.evaluate("""
        window.Keymap.register('global', {
            key: 'g', name: 'global-test', description: 'd',
            category: 'Navigation', action: function() {}
        });
        window.Keymap.register('browse', {
            key: 'b', name: 'browse-test', description: 'd',
            category: 'Edit', action: function() {}
        });
    """)

    global_only = page.evaluate("window.Keymap.shortcutsForScope('global').map(s => s.name)")
    assert global_only == ["global-test"]

    browse_scope = page.evaluate("window.Keymap.shortcutsForScope('browse').map(s => s.name)")
    # browse scope returns its own shortcuts plus globals
    assert set(browse_scope) == {"global-test", "browse-test"}


def test_dispatcher_fires_registered_action(live_server, page):
    """Pressing a registered key fires its action; suppressed when input is focused."""
    url = live_server["url"]
    page.goto(f"{url}/browse", timeout=5000)
    page.wait_for_load_state("networkidle")

    page.evaluate("""
        window._kmTestFired = 0;
        window.Keymap.register('global', {
            key: 'q', name: 'test-q', description: '', category: 'System',
            action: function() { window._kmTestFired += 1; }
        });
        window.Keymap.setScope('global');
    """)

    page.keyboard.press("q")
    assert page.evaluate("window._kmTestFired") == 1

    # Focused input suppresses the shortcut
    page.evaluate("""
        var i = document.createElement('input');
        i.id = '_kmTestInput';
        document.body.appendChild(i);
        i.focus();
    """)
    page.keyboard.press("q")
    assert page.evaluate("window._kmTestFired") == 1  # unchanged


def test_esc_stack_unwinds_top_first(live_server, page):
    """pushEsc registers handlers; Esc invokes only the top one each press."""
    url = live_server["url"]
    page.goto(f"{url}/browse", timeout=5000)
    page.wait_for_load_state("networkidle")

    page.evaluate("""
        window._kmEscOrder = [];
        window._kmEscToken1 = window.Keymap.pushEsc(function() { window._kmEscOrder.push('first'); });
        window._kmEscToken2 = window.Keymap.pushEsc(function() { window._kmEscOrder.push('second'); });
    """)

    page.keyboard.press("Escape")
    assert page.evaluate("window._kmEscOrder") == ["second"]

    page.keyboard.press("Escape")
    assert page.evaluate("window._kmEscOrder") == ["second", "first"]

    page.keyboard.press("Escape")
    assert page.evaluate("window._kmEscOrder") == ["second", "first"]  # stack empty


def test_esc_stack_remove_by_token(live_server, page):
    """popEsc(token) removes a specific handler regardless of position."""
    url = live_server["url"]
    page.goto(f"{url}/browse", timeout=5000)
    page.wait_for_load_state("networkidle")

    page.evaluate("""
        window._kmEscOrder = [];
        var t1 = window.Keymap.pushEsc(function() { window._kmEscOrder.push('first'); });
        var t2 = window.Keymap.pushEsc(function() { window._kmEscOrder.push('second'); });
        window.Keymap.popEsc(t2);
    """)

    page.keyboard.press("Escape")
    assert page.evaluate("window._kmEscOrder") == ["first"]
