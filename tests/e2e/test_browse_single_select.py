from playwright.sync_api import expect


def test_single_click_reveals_batch_bar(live_server, page):
    """Normal-click on one photo reveals the batch bar so Develop/Export/Delete
    are reachable with a single photo selected.

    Regression: updateBatchBar() previously only showed the bar when
    selectedPhotos.size > 1, leaving single-click users with no UI path to
    batch actions against the focused photo.
    """
    url = live_server["url"]
    page.goto(f"{url}/browse")

    bar = page.locator("#batchBar")
    expect(bar).to_be_hidden()

    first = page.locator(".grid-card").first
    first.wait_for(state="visible")
    first.click()

    expect(bar).to_be_visible()
    expect(page.locator("#batchCount")).to_have_text("1 selected")
    expect(page.locator("#developBtn")).to_be_visible()


def test_closing_detail_hides_batch_bar(live_server, page):
    """Closing the detail panel clears single-focus selection and hides the bar."""
    url = live_server["url"]
    page.goto(f"{url}/browse")

    first = page.locator(".grid-card").first
    first.wait_for(state="visible")
    first.click()

    bar = page.locator("#batchBar")
    expect(bar).to_be_visible()

    # Trigger closeDetail via the summary/close button inside the detail panel.
    # Falling back to pressing Escape which browse.html wires to the same path.
    page.evaluate("closeDetail()")

    expect(bar).to_be_hidden()


def test_cmd_click_single_photo_shows_bar(live_server, page):
    """Cmd-clicking one tile (size==1) now reveals the bar too.

    Previously size>1 was required; users had to cmd-click two photos before
    any batch action became reachable.
    """
    url = live_server["url"]
    page.goto(f"{url}/browse")

    first = page.locator(".grid-card").first
    first.wait_for(state="visible")
    first.click(modifiers=["Meta"])

    bar = page.locator("#batchBar")
    expect(bar).to_be_visible()
    expect(page.locator("#batchCount")).to_have_text("1 selected")


def test_clear_button_clears_single_focus(live_server, page):
    """Clear in the batch bar must hide the bar after a single-click focus.

    Regression: clearSelection() only emptied selectedPhotos, so the focused
    selectedPhotoId survived and updateBatchBar() re-showed "1 selected",
    leaving batch actions silently armed against that photo.
    """
    url = live_server["url"]
    page.goto(f"{url}/browse")

    first = page.locator(".grid-card").first
    first.wait_for(state="visible")
    first.click()

    bar = page.locator("#batchBar")
    expect(bar).to_be_visible()

    page.locator("#batchBar button", has_text="Clear").click()

    expect(bar).to_be_hidden()
    # selectedPhotoId must be cleared too so batch actions no longer target it.
    assert page.evaluate("selectedPhotoId") is None


def test_clear_button_closes_detail_panel(live_server, page):
    """Clear in the batch bar must also hide the detail panel.

    Regression: clearSelection() nulled selectedPhotoId but left the detail
    panel visible. Detail-panel handlers (setFlag, setColorLabel, addKeyword)
    early-return on null selectedPhotoId, so buttons silently did nothing
    while the panel remained on screen.
    """
    url = live_server["url"]
    page.goto(f"{url}/browse")

    first = page.locator(".grid-card").first
    first.wait_for(state="visible")
    first.click()

    # Detail panel gains the "visible" class when a photo is focused.
    page.wait_for_function(
        "document.getElementById('detailContent').classList.contains('visible')",
        timeout=2000,
    )

    page.locator("#batchBar button", has_text="Clear").click()

    # Detail panel must drop the visible class so the summary comes back.
    assert not page.evaluate(
        "document.getElementById('detailContent').classList.contains('visible')"
    )
    assert not page.evaluate(
        "document.getElementById('summaryPanel').classList.contains('hidden')"
    )


def test_cmd_click_toggles_focus_out_of_set_reconciles(live_server, page):
    """click A, cmd-click B, cmd-click A: the focus must not linger on A.

    Regression: getActiveSelection() prefers selectedPhotos over
    selectedPhotoId, so after this sequence the set was {B} while
    selectedPhotoId was still A. A remained visibly highlighted (and the
    detail panel still showed A), but batch actions silently targeted B.
    Fix: after a cmd-click toggle that removes selectedPhotoId from a
    non-empty set, clear selectedPhotoId and close the stale detail panel.
    """
    url = live_server["url"]
    page.goto(f"{url}/browse")

    cards = page.locator(".grid-card")
    cards.first.wait_for(state="visible")
    assert cards.count() >= 2

    a_id = int(cards.nth(0).get_attribute("data-id"))
    b_id = int(cards.nth(1).get_attribute("data-id"))

    cards.nth(0).click()  # click A: focus A
    cards.nth(1).click(modifiers=["Meta"])  # cmd-click B: set={A,B}, focus=A
    cards.nth(0).click(modifiers=["Meta"])  # cmd-click A: set={B}, stale focus

    # Active selection must only contain B, and the focused id must be cleared
    # so the visible highlight and getActiveSelection() agree.
    active = page.evaluate("getActiveSelection()")
    assert active == [b_id], f"expected [{b_id}], got {active}"
    assert page.evaluate("selectedPhotoId") is None

    # The stale detail panel must be hidden so its (now no-op) handlers
    # can't be invoked against a null selectedPhotoId.
    assert not page.evaluate(
        "document.getElementById('detailContent').classList.contains('visible')"
    )

    # Card A must no longer carry the "selected" highlight; card B still does.
    assert not page.evaluate(
        f"document.querySelector('.grid-card[data-id=\"{a_id}\"]').classList.contains('selected')"
    )
    assert page.evaluate(
        f"document.querySelector('.grid-card[data-id=\"{b_id}\"]').classList.contains('selected')"
    )


def test_close_detail_preserves_multiselect_highlight(live_server, page):
    """click A -> cmd-click B -> cmd-click B -> closeDetail must keep A lit.

    Regression: closeDetail() stripped .selected from every card but left
    selectedPhotos intact, so the bar kept showing "1 selected" while no
    card was visibly highlighted. Destructive batch actions (delete/export/
    develop) would then target a photo the user could no longer identify.
    Fix: re-apply the .selected class to any card still in selectedPhotos
    during closeDetail.
    """
    url = live_server["url"]
    page.goto(f"{url}/browse")

    cards = page.locator(".grid-card")
    cards.first.wait_for(state="visible")
    assert cards.count() >= 2

    a_id = int(cards.nth(0).get_attribute("data-id"))
    b_id = int(cards.nth(1).get_attribute("data-id"))

    cards.nth(0).click()  # click A: set={}, focus=A
    cards.nth(1).click(modifiers=["Meta"])  # cmd-click B: set={A,B}, focus=A
    cards.nth(1).click(modifiers=["Meta"])  # cmd-click B again: set={A}, focus=A

    page.evaluate("closeDetail()")

    # Bar must still reflect the surviving multi-select entry.
    expect(page.locator("#batchBar")).to_be_visible()
    expect(page.locator("#batchCount")).to_have_text("1 selected")
    assert page.evaluate("Array.from(selectedPhotos)") == [a_id]
    assert page.evaluate("selectedPhotoId") is None

    # Card A must still paint as selected so the user can see what will be acted on.
    assert page.evaluate(
        f"document.querySelector('.grid-card[data-id=\"{a_id}\"]').classList.contains('selected')"
    )
    assert not page.evaluate(
        f"document.querySelector('.grid-card[data-id=\"{b_id}\"]').classList.contains('selected')"
    )


def test_resetAndLoad_clears_multiselect_set(live_server, page):
    """Changing sort/filter/folder must drop a surviving multi-select set.

    Regression: resetAndLoad() cleared selectedPhotoId but left selectedPhotos
    intact, so a cmd-click selection survived sort/filter/folder changes. The
    bar would reappear in the new view with stale ids, arming delete/export/
    develop against photos that might not be present anymore.
    """
    url = live_server["url"]
    page.goto(f"{url}/browse")

    first = page.locator(".grid-card").first
    first.wait_for(state="visible")
    first.click(modifiers=["Meta"])

    bar = page.locator("#batchBar")
    expect(bar).to_be_visible()
    assert page.evaluate("selectedPhotos.size") == 1

    # Simulate any dataset-changing action (sort change, filter, folder click).
    page.evaluate("resetAndLoad()")

    assert page.evaluate("selectedPhotos.size") == 0
    assert page.evaluate("selectedPhotoId") is None
    expect(bar).to_be_hidden()
