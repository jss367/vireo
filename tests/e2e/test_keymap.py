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

    # Filter to just the test-injected names so we don't accidentally pick up
    # whatever globals the navbar bootstrap registered (nav shortcuts, etc.)
    global_only = page.evaluate(
        "window.Keymap.shortcutsForScope('global')"
        "    .map(s => s.name)"
        "    .filter(n => n === 'global-test' || n === 'browse-test')"
    )
    assert global_only == ["global-test"]

    browse_scope = page.evaluate(
        "window.Keymap.shortcutsForScope('browse')"
        "    .map(s => s.name)"
        "    .filter(n => n === 'global-test' || n === 'browse-test')"
    )
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


def test_page_scope_shadows_global_for_same_key(live_server, page):
    """When global and page scopes register the same key, page wins."""
    url = live_server["url"]
    page.goto(f"{url}/browse", timeout=5000)
    page.wait_for_load_state("networkidle")

    page.evaluate("""
        window._kmFired = '';
        window.Keymap.register('global', {
            key: 'p', name: 'g', description: '', category: 'System',
            action: function() { window._kmFired = 'global'; }
        });
        window.Keymap.register('browse', {
            key: 'p', name: 'b', description: '', category: 'Edit',
            action: function() { window._kmFired = 'page'; }
        });
        window.Keymap.setScope('browse');
    """)

    page.keyboard.press("p")
    assert page.evaluate("window._kmFired") == "page"


def test_navbar_nav_shortcuts_registered_globally(live_server, page):
    """Each NAV_ROUTES entry is registered as a global Keymap shortcut after config load."""
    url = live_server["url"]
    page.goto(f"{url}/browse", timeout=5000)
    page.wait_for_load_state("networkidle")

    # After the nav shortcut bootstrap runs, every nav entry should be in the global scope.
    names = page.evaluate("""
        window.Keymap.shortcutsForScope('global')
            .filter(s => s.category === 'Navigation')
            .map(s => s.name)
    """)
    expected = {
        'pipeline', 'lightroom', 'pipeline_review', 'review', 'cull',
        'browse', 'map', 'variants', 'dashboard', 'audit', 'compare',
        'workspace', 'shortcuts', 'settings', 'keywords'
    }
    assert expected.issubset(set(names))


def test_pressing_b_navigates_to_browse(live_server, page):
    """Pressing 'b' from a non-browse page navigates to /browse."""
    url = live_server["url"]
    page.goto(f"{url}/cull", timeout=5000)
    page.wait_for_load_state("networkidle")
    page.keyboard.press("b")
    page.wait_for_url(f"{url}/browse", timeout=3000)


def test_nav_shortcut_suppressed_when_overlay_open(live_server, page):
    """Pressing a nav letter while an overlay is open does not navigate."""
    url = live_server["url"]
    page.goto(f"{url}/cull", timeout=5000)
    page.wait_for_load_state("networkidle")

    # Inject an overlay matching the OVERLAY_SELECTOR set
    page.evaluate("""
        var ov = document.createElement('div');
        ov.className = 'modal-overlay open';
        document.body.appendChild(ov);
    """)

    page.keyboard.press("b")
    page.wait_for_timeout(400)
    assert page.url.endswith("/cull"), f"Expected to stay on /cull, got {page.url}"


def test_action_returning_false_does_not_preventdefault(live_server, page):
    """Actions returning false signal 'not handled' and let next candidate run."""
    url = live_server["url"]
    page.goto(f"{url}/browse", timeout=5000)
    page.wait_for_load_state("networkidle")

    page.evaluate("""
        window._kmCalls = [];
        window.Keymap.register('global', {
            key: 'y', name: 'first', description: '', category: 'System',
            action: function() { window._kmCalls.push('first'); return false; }
        });
        window.Keymap.register('global', {
            key: 'y', name: 'second', description: '', category: 'System',
            action: function() { window._kmCalls.push('second'); }
        });
    """)

    page.keyboard.press("y")
    assert page.evaluate("window._kmCalls") == ["first", "second"]


def test_esc_closes_shortcuts_cheat_sheet(live_server, page):
    """Opening the cheat sheet pushes an Esc handler; pressing Esc closes it."""
    url = live_server["url"]
    page.goto(f"{url}/browse", timeout=5000)
    page.wait_for_load_state("networkidle")

    page.keyboard.press("?")
    sheet = page.locator("#shortcutsCheatSheet")
    expect_open = sheet.evaluate("el => el.classList.contains('open')")
    assert expect_open is True

    page.keyboard.press("Escape")
    expect_closed = sheet.evaluate("el => el.classList.contains('open')")
    assert expect_closed is False


def test_two_overlays_unwind_one_esc_each(live_server, page):
    """With lightbox open and cheat sheet stacked on top, each Esc closes one
    overlay (top-of-stack first). Locks in the new one-Esc-per-overlay model
    that replaces the legacy 'shotgun close' Esc cascade."""
    url = live_server["url"]
    page.goto(f"{url}/browse", timeout=5000)
    page.wait_for_load_state("networkidle")

    # Open the lightbox on the first photo via the JS API directly
    page.evaluate("openLightbox(1, 'hawk1.jpg')")
    page.wait_for_function(
        "document.getElementById('lightboxOverlay').classList.contains('active')",
        timeout=2000,
    )

    # Open the shortcuts cheat sheet on top
    page.evaluate("openShortcutsSheet()")
    page.wait_for_function(
        "document.getElementById('shortcutsCheatSheet').classList.contains('open')",
        timeout=2000,
    )

    # First Esc closes only the cheat sheet (top of stack)
    page.keyboard.press("Escape")
    page.wait_for_function(
        "!document.getElementById('shortcutsCheatSheet').classList.contains('open')",
        timeout=2000,
    )
    lightbox_still_open = page.evaluate(
        "document.getElementById('lightboxOverlay').classList.contains('active')"
    )
    assert lightbox_still_open is True, "Lightbox should still be open after first Esc"

    # Second Esc closes the lightbox
    page.keyboard.press("Escape")
    page.wait_for_function(
        "!document.getElementById('lightboxOverlay').classList.contains('active')",
        timeout=2000,
    )


def test_overlay_esc_does_not_leak_to_page_handlers(live_server, page):
    """When the Esc stack handles Esc, page-level Esc handlers (e.g. browse.html
    clearing selection, lightbox close-detail) must not also fire. The capture-
    phase dispatcher + stopPropagation guarantees this."""
    url = live_server["url"]
    page.goto(f"{url}/browse", timeout=5000)
    page.wait_for_load_state("networkidle")

    # Add a body-level bubble Esc spy
    page.evaluate("""
        window.__leakCount = 0;
        document.body.addEventListener('keydown', function(e) {
            if (e.key === 'Escape') window.__leakCount += 1;
        });
    """)

    # Push an Esc handler via the stack
    page.evaluate("""
        window._testEscToken = window.Keymap.pushEsc(function() {});
    """)

    page.keyboard.press("Escape")
    assert page.evaluate("window.__leakCount") == 0, "Esc must not leak to body when handled by stack"


def test_esc_closes_help_modal_via_stack(live_server, page):
    """Help modal opened via F1 pushes an Esc handler; Esc closes it."""
    url = live_server["url"]
    page.goto(f"{url}/browse", timeout=5000)
    page.wait_for_load_state("networkidle")

    page.keyboard.press("F1")
    page.wait_for_function(
        "document.getElementById('helpModal').classList.contains('active')",
        timeout=2000,
    )

    page.keyboard.press("Escape")
    page.wait_for_function(
        "!document.getElementById('helpModal').classList.contains('active')",
        timeout=2000,
    )


def test_legacy_page_binding_shadows_global_nav(live_server, page):
    """When the current page has bound a key in the legacy _vireoShortcuts
    config, the global nav action yields so the page's bubble-phase listener
    can run. Browse/review/cull page handlers don't migrate into the registry
    until PR 4; until then this preserves user-customized collisions
    (e.g. mapping a browse action onto the same letter as a nav shortcut)."""
    url = live_server["url"]
    page.goto(f"{url}/browse", timeout=5000)
    page.wait_for_load_state("networkidle")

    # Pretend the user has remapped a browse action to 'r' (default for
    # navigation.review). Mutating after registration is fine — the nav
    # action reads _vireoShortcuts at dispatch time.
    page.evaluate("""
        window._vireoShortcuts = window._vireoShortcuts || {};
        window._vireoShortcuts.browse = window._vireoShortcuts.browse || {};
        window._vireoShortcuts.browse.flag = 'r';

        // Bubble-phase spy mimicking a legacy page listener.
        window.__pageHandled = 0;
        document.addEventListener('keydown', function(e) {
            if (e.key === 'r' && !e.defaultPrevented) window.__pageHandled += 1;
        });
    """)

    page.keyboard.press("r")
    page.wait_for_timeout(300)

    assert page.url.endswith("/browse"), f"Expected to stay on /browse, got {page.url}"
    assert page.evaluate("window.__pageHandled") == 1, (
        "Legacy bubble-phase listener should have observed 'r' with defaultPrevented=false"
    )


def test_setscope_runs_synchronously_at_page_load(live_server, page):
    """The navbar IIFE must call Keymap.setScope(pageCtx) synchronously after
    keymap.js loads. Regression test for the script-ordering bug where
    keymap.js loaded after the inline IIFE, leaving the dispatcher stuck on
    'global' scope and silently breaking page-scoped shortcuts in PR 2."""
    url = live_server["url"]
    page.goto(f"{url}/browse", timeout=5000)
    page.wait_for_load_state("networkidle")
    assert page.evaluate("window.Keymap.getScope()") == "browse"

    page.goto(f"{url}/review", timeout=5000)
    page.wait_for_load_state("networkidle")
    assert page.evaluate("window.Keymap.getScope()") == "review"
