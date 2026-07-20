"""Pointer drag-reorder of the navbar tab strip."""


def _nav_ids(page):
    return page.eval_on_selector_all(
        ".navbar .nav-tab:not(.is-ephemeral)",
        "els => els.map(e => e.dataset.navId)",
    )


def test_drag_reorder_persists_with_real_mouse(live_server, page):
    """A real native drag moves the tab and persists across reload."""
    url = live_server["url"]
    page.goto(f"{url}/browse")
    page.wait_for_selector(".nav-tab[data-nav-id='cull']")

    before = _nav_ids(page)
    assert before[0] == "import"
    assert "cull" in before

    src = page.query_selector(".nav-tab[data-nav-id='cull']")
    dst = page.query_selector(".nav-tab[data-nav-id='import']")
    sb, db = src.bounding_box(), dst.bounding_box()
    page.mouse.move(sb["x"] + sb["width"] / 2, sb["y"] + sb["height"] / 2)
    page.mouse.down()
    for i in range(1, 11):
        page.mouse.move(
            sb["x"] + (db["x"] - sb["x"]) * i / 10 + db["width"] / 2,
            db["y"] + db["height"] / 2,
            steps=2,
        )
    page.mouse.up()

    page.wait_for_timeout(300)
    assert page.url.endswith("/browse"), "drag release unexpectedly followed the tab link"
    page.reload()
    page.wait_for_selector(".nav-tab[data-nav-id='cull']")
    after = _nav_ids(page)

    assert after.index("cull") < before.index("cull"), (
        f"cull did not move earlier: before={before} after={after}"
    )


def test_reorder_uses_pointer_release_without_html_drop(live_server, page):
    """Regression: reordering must not depend on WKWebView HTML drag events.

    Drives the drag with Playwright's real mouse API (so ``setPointerCapture``
    works — synthetic ``PointerEvent`` dispatches throw ``NotFoundError``
    because their ``pointerId`` isn't a real browser pointer) while asserting
    that no ``dragstart``/``drop`` events fire on the strip. That combination
    proves the reorder happens purely through the pointer-event path.
    """
    url = live_server["url"]
    page.goto(f"{url}/browse")
    page.wait_for_selector(".nav-tab[data-nav-id='cull']")

    before = _nav_ids(page)

    page.evaluate("""() => {
        const strip = document.getElementById('navTabStrip');
        window.__htmlDragCount = 0;
        ['dragstart', 'dragover', 'drop', 'dragend'].forEach(type => {
            strip.addEventListener(type, () => { window.__htmlDragCount += 1; }, true);
        });
    }""")

    src = page.query_selector(".nav-tab[data-nav-id='cull']")
    dst = page.query_selector(".nav-tab[data-nav-id='import']")
    sb, db = src.bounding_box(), dst.bounding_box()
    page.mouse.move(sb["x"] + sb["width"] / 2, sb["y"] + sb["height"] / 2)
    page.mouse.down()
    for i in range(1, 11):
        page.mouse.move(
            sb["x"] + (db["x"] + 1 - sb["x"]) * i / 10,
            db["y"] + db["height"] / 2,
            steps=2,
        )
    page.mouse.up()

    page.wait_for_timeout(300)
    html_drag_count = page.evaluate("window.__htmlDragCount")
    assert html_drag_count == 0, (
        f"reorder should not fire HTML5 drag events, saw {html_drag_count}"
    )
    dom_after = _nav_ids(page)
    assert dom_after[0] == "cull", (
        f"tab did not move on pointer release: {dom_after}"
    )

    page.reload()
    page.wait_for_selector(".nav-tab[data-nav-id='cull']")
    after_reload = _nav_ids(page)
    assert after_reload[0] == "cull", (
        f"reorder did not persist: before={before} after={after_reload}"
    )


def test_aborted_drag_outside_strip_does_not_reorder(live_server, page):
    """Releasing outside the navbar must not persist the last inside position."""
    url = live_server["url"]
    page.goto(f"{url}/browse")
    page.wait_for_selector(".nav-tab[data-nav-id='cull']")

    before = _nav_ids(page)
    src = page.query_selector(".nav-tab[data-nav-id='cull']")
    dst = page.query_selector(".nav-tab[data-nav-id='import']")
    sb, db = src.bounding_box(), dst.bounding_box()
    start_x = sb["x"] + sb["width"] / 2
    strip_y = db["y"] + db["height"] / 2
    destination_x = db["x"] + 1
    page.mouse.move(start_x, strip_y)
    page.mouse.down()
    page.mouse.move(destination_x, strip_y, steps=10)
    page.mouse.move(destination_x, db["y"] + db["height"] + 100, steps=5)
    page.mouse.up()

    page.wait_for_timeout(300)
    dom_after = _nav_ids(page)
    assert dom_after == before, (
        f"aborted drag unexpectedly reordered tabs: before={before} after={dom_after}"
    )

    page.reload()
    page.wait_for_selector(".nav-tab[data-nav-id='cull']")
    after_reload = _nav_ids(page)
    assert after_reload == before, (
        f"aborted drag persisted a stale reorder: before={before} after_reload={after_reload}"
    )
