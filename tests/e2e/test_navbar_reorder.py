"""Drag-reorder of the navbar tab strip.

Covers the "dropped tab snaps back" bug: the reorder must be committed on
`dragend` (which fires reliably on the source element) and not depend on the
`drop` event, because the macOS WKWebView the desktop app renders in
frequently never fires `drop` for HTML5 drag-and-drop.
"""


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
    page.reload()
    page.wait_for_selector(".nav-tab[data-nav-id='cull']")
    after = _nav_ids(page)

    assert after.index("cull") < before.index("cull"), (
        f"cull did not move earlier: before={before} after={after}"
    )


def test_reorder_commits_without_drop_event(live_server, page):
    """Regression: WKWebView fires dragstart/dragover/dragend but not drop.

    Dispatch that exact event sequence (no `drop`) and assert the reorder is
    still committed and persisted. On the old drop-only code the tab snapped
    back; this must now succeed.
    """
    url = live_server["url"]
    page.goto(f"{url}/browse")
    page.wait_for_selector(".nav-tab[data-nav-id='cull']")

    before = _nav_ids(page)

    moved = page.evaluate("""() => {
        const strip = document.getElementById('navTabStrip');
        const src = strip.querySelector(".nav-tab[data-nav-id='cull']");
        const importTab = strip.querySelector(".nav-tab[data-nav-id='import']");
        const rect = importTab.getBoundingClientRect();
        // Aim just left of the first tab's midpoint so cull lands at the front.
        const clientX = rect.left + 1;

        const fire = (el, type, x) => {
            const ev = new DragEvent(type, {
                bubbles: true, cancelable: true,
                dataTransfer: new DataTransfer(), clientX: x,
            });
            el.dispatchEvent(ev);
        };
        fire(src, 'dragstart', 0);
        fire(strip, 'dragover', clientX);
        fire(src, 'dragend', clientX);   // NOTE: no 'drop' event
        return true;
    }""")
    assert moved

    page.wait_for_timeout(300)
    dom_after = _nav_ids(page)
    assert dom_after[0] == "cull", (
        f"tab did not move on dragend without drop: {dom_after}"
    )

    page.reload()
    page.wait_for_selector(".nav-tab[data-nav-id='cull']")
    after_reload = _nav_ids(page)
    assert after_reload[0] == "cull", (
        f"reorder did not persist: before={before} after={after_reload}"
    )
