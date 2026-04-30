from playwright.sync_api import expect


def test_navbar_links_navigate_to_pipeline(live_server, page):
    """Clicking Pipeline in navbar navigates to the pipeline page."""
    url = live_server["url"]
    page.goto(f"{url}/browse")
    page.click("[data-testid='nav-pipeline']")
    expect(page).to_have_url(f"{url}/pipeline")


def test_navbar_links_navigate_to_jobs(live_server, page):
    """Clicking Jobs in navbar navigates to the jobs page."""
    url = live_server["url"]
    page.goto(f"{url}/browse")
    page.click("[data-testid='nav-jobs']")
    expect(page).to_have_url(f"{url}/jobs")


def test_navbar_links_navigate_to_browse(live_server, page):
    """Clicking Browse in navbar navigates to the browse page."""
    url = live_server["url"]
    page.goto(f"{url}/pipeline")
    page.click("[data-testid='nav-browse']")
    expect(page).to_have_url(f"{url}/browse")


def test_workspace_dropdown_shows_current(live_server, page):
    """Workspace dropdown displays the current workspace name."""
    url = live_server["url"]
    page.goto(f"{url}/browse")
    dropdown = page.locator("[data-testid='workspace-dropdown']")
    expect(dropdown).to_contain_text("Default")


def test_workspace_switch(live_server, page):
    """Switching workspace updates the dropdown to show the new workspace."""
    url = live_server["url"]
    page.goto(f"{url}/browse")
    page.click("[data-testid='workspace-dropdown']")
    # Wait for workspace menu items to load (fetched via JS)
    page.locator(".ws-menu-item", has_text="Field Work").wait_for(state="visible")
    page.locator(".ws-menu-item", has_text="Field Work").click()
    page.wait_for_load_state("networkidle")
    dropdown = page.locator("[data-testid='workspace-dropdown']")
    expect(dropdown).to_contain_text("Field Work")


def test_workspace_persists_across_navigation(live_server, page):
    """After switching workspace, navigating to another page keeps the workspace."""
    url = live_server["url"]
    page.goto(f"{url}/browse")
    page.click("[data-testid='workspace-dropdown']")
    # Wait for workspace menu items to load (fetched via JS)
    page.locator(".ws-menu-item", has_text="Field Work").wait_for(state="visible")
    page.locator(".ws-menu-item", has_text="Field Work").click()
    page.wait_for_load_state("networkidle")
    page.click("[data-testid='nav-pipeline']")
    page.wait_for_load_state("networkidle")
    dropdown = page.locator("[data-testid='workspace-dropdown']")
    expect(dropdown).to_contain_text("Field Work")


def test_navbar_renders_default_tabs_dynamically(live_server, page):
    """The 9 default tabs render as <a class='nav-tab'> dynamically — no
    static linger-page anchors, no '+ Tools' button."""
    url = live_server["url"]
    page.goto(f"{url}/browse")
    page.wait_for_selector(".nav-tab[data-nav-id='browse']")
    nav_ids = page.eval_on_selector_all(
        ".navbar .nav-tab",
        "els => els.map(e => e.dataset.navId)"
    )
    assert "browse" in nav_ids
    assert "pipeline" in nav_ids
    assert "review" in nav_ids
    # No tools button
    assert page.query_selector(".nav-tools-btn") is None
    # No standalone Logs icon (it's now a tab if pinned)
    logs_icons = page.query_selector_all(".nav-icon[href='/logs']")
    assert len(logs_icons) == 0


def test_pinning_a_tab_via_api_makes_it_appear_in_strip(live_server, page):
    url = live_server["url"]
    page.goto(f"{url}/browse")
    # Initially no `logs` tab
    assert page.query_selector(".nav-tab[data-nav-id='logs']") is None
    page.evaluate("""async () => {
        await fetch('/api/workspace/tabs/pin',
                    {method:'POST', headers:{'Content-Type':'application/json'},
                     body: JSON.stringify({nav_id:'logs'})});
    }""")
    page.reload()
    page.wait_for_selector(".nav-tab[data-nav-id='logs']", timeout=3000)


def test_unpinning_active_tab_navigates_to_adjacent(live_server, page):
    url = live_server["url"]
    page.goto(f"{url}/cull")  # active tab is 'cull'
    page.wait_for_selector(".nav-tab[data-nav-id='cull'].active")
    page.click(".nav-tab[data-nav-id='cull'] .nav-tab-close")
    page.wait_for_load_state("networkidle")
    # Navigated to a sibling — anything that isn't /cull
    assert "/cull" not in page.url


def test_hotkey_underline_appears_on_pinned_tabs(live_server, page):
    """Pinned tabs in the dynamic strip get hotkey underlines (.hk span)."""
    url = live_server["url"]
    page.goto(f"{url}/browse")
    page.wait_for_selector(".nav-tab[data-nav-id='browse']")
    # Wait for hotkey hints to apply (they're computed after tab render)
    page.wait_for_selector(".nav-tab[data-nav-id='browse'] .hk", timeout=2000)


def test_tab_close_button_does_not_change_tab_width_on_hover(live_server, page):
    """Hovering a tab in the navbar must not change the tab's bounding box.

    The bounce bug was: hover → close button shows via display change → tab
    grows wider → flex re-layout. With absolute positioning the tab width
    is fixed regardless of hover.
    """
    url = live_server["url"]
    page.set_viewport_size({"width": 1366, "height": 800})
    page.goto(f"{url}/browse")
    # Pin a known tab so it's in the strip
    page.evaluate("""async () => {
        await fetch('/api/workspace/tabs/pin',
                    {method:'POST', headers:{'Content-Type':'application/json'},
                     body: JSON.stringify({nav_id:'logs'})});
    }""")
    page.reload()
    page.wait_for_selector(".nav-tab[data-nav-id='logs']")
    tab = page.query_selector(".nav-tab[data-nav-id='logs']")
    box_before = tab.bounding_box()
    # Hover the tab
    page.mouse.move(box_before["x"] + box_before["width"] / 2,
                    box_before["y"] + box_before["height"] / 2)
    page.wait_for_timeout(150)
    box_after = tab.bounding_box()
    assert abs(box_before["width"] - box_after["width"]) < 1.0, \
        f"Tab width changed on hover ({box_before['width']} → {box_after['width']})"
    assert abs(box_before["height"] - box_after["height"]) < 1.0, \
        f"Tab height changed on hover ({box_before['height']} → {box_after['height']})"


def test_cmdk_opens_palette(live_server, page):
    url = live_server["url"]
    page.goto(f"{url}/browse")
    # Modal initially hidden
    palette = page.query_selector("#commandPalette")
    assert palette is not None
    assert palette.is_hidden()
    # Cmd+K (mac) or Ctrl+K elsewhere
    page.keyboard.press("Meta+K")
    page.wait_for_selector("#commandPalette:not([hidden])", timeout=2000)
    # Esc closes
    page.keyboard.press("Escape")
    page.wait_for_function(
        "() => document.getElementById('commandPalette').hasAttribute('hidden')",
        timeout=2000,
    )


def test_palette_filters_by_query(live_server, page):
    url = live_server["url"]
    page.goto(f"{url}/browse")
    page.keyboard.press("Meta+K")
    page.wait_for_selector("#commandPalette:not([hidden])")
    page.fill("#cmdPaletteInput", "dup")
    # Wait for Duplicates row to be the (only/top) result
    page.wait_for_selector(".cmd-palette-result[data-nav-id='duplicates']", timeout=2000)


def test_palette_enter_navigates(live_server, page):
    url = live_server["url"]
    page.goto(f"{url}/browse")
    page.keyboard.press("Meta+K")
    page.fill("#cmdPaletteInput", "dup")
    page.wait_for_selector(".cmd-palette-result[data-nav-id='duplicates'].selected")
    page.keyboard.press("Enter")
    page.wait_for_url(f"{url}/duplicates", timeout=3000)


def test_palette_arrow_keys_change_selection(live_server, page):
    url = live_server["url"]
    page.goto(f"{url}/browse")
    page.keyboard.press("Meta+K")
    # Empty query → all 20 pages, selected=top
    first = page.eval_on_selector(".cmd-palette-result.selected", "el => el.dataset.navId")
    page.keyboard.press("ArrowDown")
    second = page.eval_on_selector(".cmd-palette-result.selected", "el => el.dataset.navId")
    assert first != second


def test_cmd1_jumps_to_first_pinned_tab(live_server, page):
    url = live_server["url"]
    page.goto(f"{url}/jobs")  # start somewhere not first
    # Wait for the dynamic tab strip to render so window._navTabs is populated.
    page.wait_for_selector(".nav-tab[data-nav-id='browse']", timeout=3000)
    page.keyboard.press("Meta+1")
    page.wait_for_url(f"{url}/browse", timeout=3000)


def test_drag_reorder_persists_via_reorder_endpoint(live_server, page):
    url = live_server["url"]
    page.goto(f"{url}/browse")
    page.wait_for_selector(".nav-tab[data-nav-id='browse']")
    # Move 'browse' to after 'review' via the public reorder endpoint shape.
    # (We test the JS hook, not raw mouse drag — drag in headless is flaky.)
    page.evaluate("""async () => {
        const tabs = window._navTabs.getTabs();
        const a = tabs.indexOf('browse');
        const b = tabs.indexOf('review');
        if (a < 0 || b < 0) throw new Error('expected default tabs');
        const next = tabs.slice();
        next.splice(a, 1);
        next.splice(b, 0, 'browse');
        await window._navTabs.setTabs(next);
    }""")
    page.wait_for_function("""() => {
        const t = window._navTabs.getTabs();
        return t.indexOf('browse') > t.indexOf('review');
    }""", timeout=3000)
