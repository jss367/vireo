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
