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
