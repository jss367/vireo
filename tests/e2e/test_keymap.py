"""End-to-end tests for the keymap registry and dispatcher."""



def test_keymap_globals_exposed(live_server, page):
    """Loading any page exposes the Keymap module on window."""
    url = live_server["url"]
    page.goto(f"{url}/browse", timeout=15000)
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
    page.goto(f"{url}/browse", timeout=15000)
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
    page.goto(f"{url}/browse", timeout=15000)
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
    page.goto(f"{url}/browse", timeout=15000)
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
    page.goto(f"{url}/browse", timeout=15000)
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
    page.goto(f"{url}/browse", timeout=15000)
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


def test_default_navigation_shortcuts_are_not_registered_as_bare_keys(live_server, page):
    """Default navigation does not reserve bare keys in the global dispatcher."""
    url = live_server["url"]
    page.goto(f"{url}/browse", timeout=15000)
    page.wait_for_load_state("networkidle")

    names = page.evaluate("""
        window.Keymap.shortcutsForScope('global')
            .filter(s => s.category === 'Navigation')
            .map(s => s.name)
    """)
    assert names == []


def test_legacy_bare_navigation_shortcut_is_ignored(live_server, page):
    """Existing configs with bare nav letters must not steal page-local keys."""
    page.route(
        "**/api/config",
        lambda route: route.fulfill(
            json={"keyboard_shortcuts": {"navigation": {"browse": "b"}}}
        ),
    )
    url = live_server["url"]
    page.goto(f"{url}/cull", timeout=15000)
    page.wait_for_load_state("networkidle")
    page.keyboard.press("b")
    page.wait_for_timeout(300)
    assert page.url.endswith("/cull"), f"Expected to stay on /cull, got {page.url}"


def test_modified_navigation_shortcut_still_navigates(live_server, page):
    """Modifier chords remain valid for explicit navigation shortcuts."""
    page.route(
        "**/api/config",
        lambda route: route.fulfill(
            json={"keyboard_shortcuts": {"navigation": {"browse": "ctrl+b"}}}
        ),
    )
    url = live_server["url"]
    page.goto(f"{url}/cull", timeout=15000)
    page.wait_for_load_state("networkidle")
    page.keyboard.press("Control+B")
    page.wait_for_url(f"{url}/browse", timeout=3000)


def test_nav_shortcut_suppressed_when_overlay_open(live_server, page):
    """A modified nav shortcut is suppressed while an overlay is open."""
    page.route(
        "**/api/config",
        lambda route: route.fulfill(
            json={"keyboard_shortcuts": {"navigation": {"browse": "ctrl+b"}}}
        ),
    )
    url = live_server["url"]
    page.goto(f"{url}/cull", timeout=15000)
    page.wait_for_load_state("networkidle")

    # Inject an overlay matching the OVERLAY_SELECTOR set
    page.evaluate("""
        var ov = document.createElement('div');
        ov.className = 'modal-overlay open';
        document.body.appendChild(ov);
    """)

    page.keyboard.press("Control+B")
    page.wait_for_timeout(400)
    assert page.url.endswith("/cull"), f"Expected to stay on /cull, got {page.url}"


def test_action_returning_false_does_not_preventdefault(live_server, page):
    """Actions returning false signal 'not handled' and let next candidate run."""
    url = live_server["url"]
    page.goto(f"{url}/browse", timeout=15000)
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
    page.goto(f"{url}/browse", timeout=15000)
    page.wait_for_load_state("networkidle")

    page.keyboard.press("?")
    sheet = page.locator("#shortcutsCheatSheet")
    expect_open = sheet.evaluate("el => el.classList.contains('open')")
    assert expect_open is True

    page.keyboard.press("Escape")
    expect_closed = sheet.evaluate("el => el.classList.contains('open')")
    assert expect_closed is False


def test_shortcuts_sheet_lists_misses_and_shared_hotkeys(live_server, page):
    """The ? sheet should include /misses-only keys plus contextual shared keys."""
    url = live_server["url"]
    page.goto(f"{url}/misses", timeout=15000)
    page.wait_for_load_state("networkidle")
    page.wait_for_function("window._vireoShortcuts && window._vireoShortcuts.browse")

    page.evaluate("""
        window._vireoShortcuts.browse.flag = 'alt+p';
        window._vireoShortcuts.browse.reject = 'alt+x';
        window._vireoShortcuts.browse.unflag = 'alt+u';
    """)
    page.keyboard.press("?")
    page.wait_for_function(
        "document.getElementById('shortcutsCheatSheet').classList.contains('open')",
        timeout=2000,
    )

    groups = page.evaluate("""
        () => {
          const out = {};
          let title = null;
          document.querySelectorAll('#shortcutsSheetContent > div').forEach((el) => {
            if (el.classList.contains('sc-group-title')) {
              title = el.textContent.trim();
              out[title] = [];
            } else if (title && el.classList.contains('sc-row')) {
              out[title].push({
                key: el.querySelector('.sc-key').textContent.trim(),
                label: el.querySelector('.sc-label').textContent.trim(),
              });
            }
          });
          return out;
        }
    """)

    assert {"Global", "Misses", "Lightbox"}.issubset(groups.keys())
    assert {"key": "J", "label": "Next miss"} in groups["Misses"]
    assert {"key": "K", "label": "Previous miss"} in groups["Misses"]
    assert {"key": "Shift+J", "label": "Extend selection to next miss"} in groups["Misses"]
    assert {"key": "Shift+K", "label": "Extend selection to previous miss"} in groups["Misses"]
    assert {"key": "Alt+P", "label": "Flag focused or selected photo"} in groups["Misses"]
    assert {"key": "Alt+X", "label": "Reject focused or selected photo"} in groups["Misses"]
    assert {"key": "Alt+U", "label": "Unmark as missed"} in groups["Misses"]
    assert {"key": "Enter", "label": "Open focused photo"} in groups["Misses"]
    assert {"key": "Escape", "label": "Clear selection"} in groups["Misses"]
    assert {"key": "?", "label": "Open keyboard shortcuts"} in groups["Global"]
    assert {"key": "Alt+U", "label": "Clear pick/reject flag"} in groups["Lightbox"]


def test_question_mark_opens_shortcuts_sheet_over_lightbox(live_server, page):
    """The global ? shortcut should still work while the lightbox owns focus,
    and the sheet must render visibly above the lightbox (not behind it)."""
    url = live_server["url"]
    page.goto(f"{url}/browse", timeout=15000)
    page.wait_for_load_state("networkidle")

    page.evaluate("openLightbox(1, 'hawk1.jpg')")
    page.wait_for_function(
        "document.getElementById('lightboxOverlay').classList.contains('active')",
        timeout=2000,
    )

    page.keyboard.press("?")
    page.wait_for_function(
        "document.getElementById('shortcutsCheatSheet').classList.contains('open')",
        timeout=2000,
    )

    assert page.evaluate("""
        () => {
          const el = document.elementFromPoint(window.innerWidth / 2, window.innerHeight / 2);
          return !!(el && el.closest('#shortcutsCheatSheet'));
        }
    """) is True
    assert page.evaluate(
        "document.getElementById('lightboxOverlay').classList.contains('active')"
    ) is True

    # Verify the sheet is visually above the lightbox, not hidden behind it.
    # A class of `open` alone is insufficient when z-index puts the sheet below
    # the lightbox overlay — see Codex review on PR #926.
    z_indexes = page.evaluate(
        """() => {
            const sheet = document.getElementById('shortcutsCheatSheet');
            const lb = document.getElementById('lightboxOverlay');
            return {
                sheet: parseInt(getComputedStyle(sheet).zIndex, 10),
                lightbox: parseInt(getComputedStyle(lb).zIndex, 10),
            };
        }"""
    )
    assert z_indexes["sheet"] > z_indexes["lightbox"], (
        f"shortcuts sheet z-index ({z_indexes['sheet']}) must exceed "
        f"lightbox z-index ({z_indexes['lightbox']}) so the sheet renders on top"
    )

    # The topmost element at the sheet's center must belong to the sheet's
    # subtree, not the lightbox — proves the stacking actually works in render.
    topmost_belongs_to_sheet = page.evaluate(
        """() => {
            const sheet = document.getElementById('shortcutsCheatSheet');
            const r = sheet.getBoundingClientRect();
            const el = document.elementFromPoint(
                r.left + r.width / 2,
                r.top + r.height / 2,
            );
            return el !== null && sheet.contains(el);
        }"""
    )
    assert topmost_belongs_to_sheet, (
        "topmost element at the sheet's center should be inside the sheet, "
        "not the lightbox behind it"
    )


def test_two_overlays_unwind_one_esc_each(live_server, page):
    """With lightbox open and cheat sheet stacked on top, each Esc closes one
    overlay (top-of-stack first). Locks in the new one-Esc-per-overlay model
    that replaces the legacy 'shotgun close' Esc cascade."""
    url = live_server["url"]
    page.goto(f"{url}/browse", timeout=15000)
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


def test_body_scroll_stays_locked_while_lower_overlay_open(live_server, page):
    """With two overlays stacked, closing the top one must not unlock body
    scroll while the lower one is still open. Regression test for the bug
    where each close*() unconditionally cleared document.body.style.overflow,
    letting the page behind an active overlay scroll after one Esc."""
    url = live_server["url"]
    page.goto(f"{url}/browse", timeout=15000)
    page.wait_for_load_state("networkidle")

    page.evaluate("openLightbox(1, 'hawk1.jpg')")
    page.wait_for_function(
        "document.getElementById('lightboxOverlay').classList.contains('active')",
        timeout=2000,
    )
    assert page.evaluate("document.body.style.overflow") == "hidden"

    page.evaluate("openShortcutsSheet()")
    page.wait_for_function(
        "document.getElementById('shortcutsCheatSheet').classList.contains('open')",
        timeout=2000,
    )
    assert page.evaluate("document.body.style.overflow") == "hidden"

    # Closing the top overlay must NOT unlock scroll — lightbox is still open.
    page.keyboard.press("Escape")
    page.wait_for_function(
        "!document.getElementById('shortcutsCheatSheet').classList.contains('open')",
        timeout=2000,
    )
    assert page.evaluate("document.body.style.overflow") == "hidden", (
        "Body scroll must remain locked while the lower overlay (lightbox) is still open"
    )

    # Closing the last overlay restores scroll.
    page.keyboard.press("Escape")
    page.wait_for_function(
        "!document.getElementById('lightboxOverlay').classList.contains('active')",
        timeout=2000,
    )
    assert page.evaluate("document.body.style.overflow") == ""


def test_overlay_esc_does_not_leak_to_page_handlers(live_server, page):
    """When the Esc stack handles Esc, page-level Esc handlers (e.g. browse.html
    clearing selection, lightbox close-detail) must not also fire. The capture-
    phase dispatcher + stopPropagation guarantees this."""
    url = live_server["url"]
    page.goto(f"{url}/browse", timeout=15000)
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
    page.goto(f"{url}/browse", timeout=15000)
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
    page.goto(f"{url}/browse", timeout=15000)
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
    page.goto(f"{url}/browse", timeout=15000)
    page.wait_for_load_state("networkidle")
    assert page.evaluate("window.Keymap.getScope()") == "browse"

    page.goto(f"{url}/review", timeout=15000)
    page.wait_for_load_state("networkidle")
    assert page.evaluate("window.Keymap.getScope()") == "review"


def test_pause_dispatch_silences_global_actions(live_server, page):
    """While dispatch is paused, registered global actions must not fire — so
    the /shortcuts editor's capture-phase keydown listener can claim the next
    press without nav shortcuts navigating the user away first. resumeDispatch
    restores normal handling."""
    url = live_server["url"]
    page.goto(f"{url}/browse", timeout=15000)
    page.wait_for_load_state("networkidle")

    page.evaluate("""
        window._kmPausedFired = 0;
        window.Keymap.register('global', {
            key: 'z', name: 'pause-test', description: '', category: 'System',
            action: function() { window._kmPausedFired += 1; }
        });
    """)

    page.evaluate("window.Keymap.pauseDispatch()")
    page.keyboard.press("z")
    assert page.evaluate("window._kmPausedFired") == 0, (
        "Paused dispatcher must not fire registered actions"
    )

    page.evaluate("window.Keymap.resumeDispatch()")
    page.keyboard.press("z")
    assert page.evaluate("window._kmPausedFired") == 1


def test_shortcut_capture_does_not_navigate(live_server, page):
    """On /shortcuts, clicking a binding button enters capture mode. Pressing
    a key that's also a global nav letter (e.g. 'b' for browse) must be
    captured as the new binding instead of navigating the user away. Regression
    test for the bug where the global Keymap dispatcher won the capture-phase
    race against the editor's own keydown listener."""
    url = live_server["url"]
    page.goto(f"{url}/shortcuts", timeout=15000)
    page.wait_for_load_state("networkidle")

    # Start capture on the navigation.browse button — pressing 'b' is then a
    # no-op rebind to itself (findConflict skips the current action), so we
    # avoid touching unrelated config while still exercising the dispatcher
    # pause path.
    page.wait_for_function(
        "document.querySelector('.shortcut-key-btn') !== null", timeout=2000
    )
    page.evaluate("""
        (function() {
            var btns = document.querySelectorAll('.shortcut-key-btn');
            for (var i = 0; i < btns.length; i++) {
                var attr = btns[i].getAttribute('onclick') || '';
                if (attr.indexOf("'navigation', 'browse'") !== -1) {
                    btns[i].click();
                    return;
                }
            }
            throw new Error('navigation.browse button not found');
        })();
    """)

    page.wait_for_function(
        "document.querySelector('.shortcut-key-btn.capturing') !== null", timeout=2000
    )

    page.keyboard.press("b")
    page.wait_for_timeout(300)

    assert page.url.endswith("/shortcuts"), (
        f"Expected to stay on /shortcuts during capture, got {page.url}"
    )


def test_help_modal_unlocks_scroll_when_keymap_unavailable(live_server, page):
    """If keymap.js fails to load (or hasn't yet), openHelpModal/closeHelpModal
    must still pair their body-scroll lock/unlock cleanly. Regression test for
    the bug where wasOpen was inferred from the Esc token (only set when
    Keymap is present), so closing the modal left body scroll locked forever."""
    url = live_server["url"]
    page.goto(f"{url}/browse", timeout=15000)
    page.wait_for_load_state("networkidle")

    # Simulate Keymap being unavailable (e.g. keymap.js failed to load).
    page.evaluate("window.Keymap = undefined; window._helpEscToken = null;")

    page.evaluate("openHelpModal()")
    page.wait_for_function(
        "document.getElementById('helpModal').classList.contains('active')",
        timeout=2000,
    )
    assert page.evaluate("document.body.style.overflow") == "hidden"

    page.evaluate("closeHelpModal()")
    page.wait_for_function(
        "!document.getElementById('helpModal').classList.contains('active')",
        timeout=2000,
    )
    assert page.evaluate("document.body.style.overflow") == "", (
        "Body scroll must unlock when help modal closes, even with Keymap unavailable"
    )
