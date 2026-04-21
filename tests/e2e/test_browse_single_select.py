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
