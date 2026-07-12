"""Browse sidebar resizing behavior."""

from playwright.sync_api import expect


def test_browse_sidebar_can_be_dragged_wider_and_remembers_width(live_server, page):
    page.goto(live_server["url"] + "/browse")

    sidebar = page.locator("#browseSidebar")
    resizer = page.locator("#browseSidebarResizer")
    expect(resizer).to_be_visible()

    initial_width = sidebar.bounding_box()["width"]
    handle_box = resizer.bounding_box()
    page.mouse.move(
        handle_box["x"] + handle_box["width"] / 2,
        handle_box["y"] + min(100, handle_box["height"] / 2),
    )
    page.mouse.down()
    page.mouse.move(
        handle_box["x"] + handle_box["width"] / 2 + 140,
        handle_box["y"] + min(100, handle_box["height"] / 2),
    )
    page.mouse.up()

    expanded_width = sidebar.bounding_box()["width"]
    assert expanded_width >= initial_width + 130
    expect(resizer).to_have_attribute("aria-valuenow", str(round(expanded_width)))

    page.reload()
    assert abs(sidebar.bounding_box()["width"] - expanded_width) <= 1


def test_browse_sidebar_resizer_supports_keyboard_controls(live_server, page):
    page.goto(live_server["url"] + "/browse")

    sidebar = page.locator("#browseSidebar")
    resizer = page.locator("#browseSidebarResizer")
    initial_width = sidebar.bounding_box()["width"]

    resizer.focus()
    resizer.press("ArrowRight")

    assert abs(sidebar.bounding_box()["width"] - (initial_width + 10)) <= 1
    expect(resizer).to_have_attribute("aria-valuenow", str(round(initial_width + 10)))


def test_browse_sidebar_resizer_keys_do_not_move_grid_selection(live_server, page):
    # ArrowLeft/ArrowRight on the resize handle also drive the page-level
    # Browse shortcut handler (moveBrowseSelection), so without
    # stopPropagation the advertised keyboard resize control would
    # simultaneously select or move to a photo and pop the detail panel.
    page.goto(live_server["url"] + "/browse")

    page.locator(".grid-card").first.wait_for(state="visible")
    assert page.evaluate("selectedIndex") == -1

    resizer = page.locator("#browseSidebarResizer")
    resizer.focus()
    resizer.press("ArrowRight")
    resizer.press("ArrowLeft")

    assert page.evaluate("selectedIndex") == -1
    assert page.evaluate("selectedPhotoId") is None
    assert not page.evaluate(
        "document.getElementById('detailContent').classList.contains('visible')"
    )


def test_browse_sidebar_max_width_reserves_room_for_detail_panel(live_server, page):
    # On a viewport this narrow, the naive `innerWidth - 360` cap would allow
    # the sidebar to grow to 600px, which — combined with the always-visible
    # 340px detail panel — would leave under 100px for the photo grid. Assert
    # the cap actually reserves enough space for the grid.
    page.set_viewport_size({"width": 1024, "height": 768})
    page.goto(live_server["url"] + "/browse")

    sidebar = page.locator("#browseSidebar")
    resizer = page.locator("#browseSidebarResizer")
    detail_panel = page.locator("#detailPanel")
    expect(detail_panel).to_be_visible()

    resizer.focus()
    resizer.press("End")

    sidebar_width = sidebar.bounding_box()["width"]
    detail_width = detail_panel.bounding_box()["width"]
    resizer_width = resizer.bounding_box()["width"]
    remaining = 1024 - sidebar_width - detail_width - resizer_width
    assert remaining >= 300, (
        f"Sidebar cap left only {remaining}px for the photo grid "
        f"(sidebar={sidebar_width}, detail={detail_width}, handle={resizer_width})"
    )
