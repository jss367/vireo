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
