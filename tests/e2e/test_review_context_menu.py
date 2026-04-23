from playwright.sync_api import expect


def test_review_card_right_click_opens_menu(live_server, page):
    """Right-clicking a review prediction card opens the context menu."""
    url = live_server["url"]
    page.goto(f"{url}/review")

    card = page.locator(".card[data-pred-id]").first
    card.wait_for(state="visible")
    card.click(button="right")

    menu = page.locator(".vireo-ctx-menu")
    expect(menu).to_be_visible()

    # Core prediction actions name the species.
    expect(menu.locator(".vireo-ctx-item", has_text="Accept as")).to_be_visible()
    expect(menu.locator(".vireo-ctx-item", has_text="Not ")).to_be_visible()

    # Navigation + file-system actions.
    expect(menu.locator(".vireo-ctx-item", has_text="Open in Lightbox")).to_be_visible()
    expect(menu.locator(".vireo-ctx-item", has_text="Reveal in")).to_be_visible()
    expect(menu.locator(".vireo-ctx-item", has_text="Copy Path")).to_be_visible()

    # Rating + flag chip rows present (6 rating + 3 flag = 9 chips).
    assert menu.locator(".vireo-ctx-chip").count() >= 9


def test_review_card_accept_fires_endpoint(live_server, page):
    """Clicking the Accept menu item POSTs to /predictions/<id>/accept."""
    url = live_server["url"]
    page.goto(f"{url}/review")

    card = page.locator(".card[data-pred-id]").first
    card.wait_for(state="visible")
    pred_id = int(card.get_attribute("data-pred-id"))
    card.click(button="right")

    menu = page.locator(".vireo-ctx-menu")
    expect(menu).to_be_visible()

    with page.expect_response(
        lambda r: f"/api/predictions/{pred_id}/accept" in r.url and r.status == 200
    ):
        menu.locator(".vireo-ctx-item", has_text="Accept as").click()
    expect(menu).to_be_hidden()


def test_review_card_reject_fires_endpoint(live_server, page):
    """Clicking the reject menu item POSTs to /predictions/<id>/reject."""
    url = live_server["url"]
    page.goto(f"{url}/review")

    card = page.locator(".card[data-pred-id]").first
    card.wait_for(state="visible")
    pred_id = int(card.get_attribute("data-pred-id"))
    card.click(button="right")

    menu = page.locator(".vireo-ctx-menu")
    expect(menu).to_be_visible()

    with page.expect_response(
        lambda r: f"/api/predictions/{pred_id}/reject" in r.url and r.status == 200
    ):
        menu.locator(".vireo-ctx-item", has_text="Not ").click()
    expect(menu).to_be_hidden()


def test_review_card_rating_chip_fires_batch_endpoint(live_server, page):
    """Clicking a rating chip applies the rating via /api/batch/rating."""
    url = live_server["url"]
    page.goto(f"{url}/review")

    card = page.locator(".card[data-pred-id]").first
    card.wait_for(state="visible")
    card.click(button="right")

    menu = page.locator(".vireo-ctx-menu")
    expect(menu).to_be_visible()

    with page.expect_response(
        lambda r: "/api/batch/rating" in r.url and r.status == 200
    ):
        menu.locator(".vireo-ctx-chip", has_text="3").first.click()
    expect(menu).to_be_hidden()


def test_review_card_reveal_fires_endpoint(live_server, page):
    """Reveal in Finder/Folder posts the right photo_id to the reveal API."""
    url = live_server["url"]
    page.goto(f"{url}/review")

    card = page.locator(".card[data-pred-id]").first
    card.wait_for(state="visible")
    card.click(button="right")

    menu = page.locator(".vireo-ctx-menu")
    expect(menu).to_be_visible()

    with page.expect_response(
        lambda r: "/api/files/reveal" in r.url and r.status == 200
    ):
        menu.locator(".vireo-ctx-item", has_text="Reveal in").click()
    expect(menu).to_be_hidden()


def test_review_card_open_lightbox_opens_overlay(live_server, page):
    """Open in Lightbox opens the shared lightbox overlay."""
    url = live_server["url"]
    page.goto(f"{url}/review")

    card = page.locator(".card[data-pred-id]").first
    card.wait_for(state="visible")
    card.click(button="right")

    menu = page.locator(".vireo-ctx-menu")
    expect(menu).to_be_visible()
    menu.locator(".vireo-ctx-item", has_text="Open in Lightbox").click()

    expect(page.locator("#lightboxOverlay")).to_be_visible()


def test_review_lightbox_rating_chip_posts_batch(live_server, page):
    """Regression guard: rating chips in the lightbox context menu on /review
    must POST to /api/batch/rating via setReviewRating. Previously the menu
    called setRatingFor (browse-only) and silently no-oped on /review.
    The color row is also omitted on /review since there is no
    setColorLabelFor equivalent.
    """
    url = live_server["url"]
    page.goto(f"{url}/review")

    card = page.locator(".card[data-pred-id]").first
    card.wait_for(state="visible")
    card.click(button="right")
    menu = page.locator(".vireo-ctx-menu")
    expect(menu).to_be_visible()
    menu.locator(".vireo-ctx-item", has_text="Open in Lightbox").click()
    expect(page.locator("#lightboxOverlay")).to_be_visible()

    # Wait for the lightbox to settle on a photo id.
    page.wait_for_function(
        "typeof _lightboxCurrentId !== 'undefined' && _lightboxCurrentId !== null",
        timeout=3000,
    )

    # Dispatch contextmenu directly — the in-test image has no real src so
    # Playwright's visibility check would stall.
    page.evaluate(
        """
        const img = document.getElementById('lightboxImg');
        const evt = new MouseEvent('contextmenu', {
            bubbles: true, cancelable: true, clientX: 300, clientY: 300,
            button: 2,
        });
        img.dispatchEvent(evt);
        """
    )
    lb_menu = page.locator(".vireo-ctx-menu")
    expect(lb_menu).to_be_visible()

    # With no setColorLabelFor on /review, only ratings (6) + flags (3) = 9.
    assert lb_menu.locator(".vireo-ctx-chip").count() == 9

    with page.expect_response(
        lambda r: "/api/batch/rating" in r.url and r.status == 200
    ):
        lb_menu.locator(".vireo-ctx-chip", has_text="4").first.click()
    expect(lb_menu).to_be_hidden()
