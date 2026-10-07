import re
from urllib.parse import quote

import pytest
from playwright.sync_api import expect


def test_review_page_loads_with_predictions(live_server, page):
    """Review page renders prediction cards for seeded species."""
    url = live_server["url"]
    page.goto(f"{url}/review", timeout=5000)

    # Wait for JS to fetch /api/predictions and render the card grid
    card = page.locator("[data-pred-id]").first
    card.wait_for(state="visible", timeout=5000)

    cards = page.locator("[data-pred-id]")
    count = cards.count()
    assert count >= 2, f"Expected at least 2 prediction cards, got {count}"


def test_review_page_shows_seeded_species(live_server, page):
    """Review page displays the seeded species names in prediction cards."""
    url = live_server["url"]
    page.goto(f"{url}/review", timeout=5000)

    # Wait for prediction cards to render
    page.locator("[data-pred-id]").first.wait_for(state="visible", timeout=5000)

    # Each card shows species in .card-prediction
    species_elements = page.locator(".card-prediction")
    species_texts = [species_elements.nth(i).text_content() for i in range(species_elements.count())]

    assert any("Red-tailed Hawk" in t for t in species_texts), (
        f"Expected 'Red-tailed Hawk' in prediction cards, got: {species_texts}"
    )
    assert any("American Robin" in t for t in species_texts), (
        f"Expected 'American Robin' in prediction cards, got: {species_texts}"
    )


def test_review_page_shows_confidence(live_server, page):
    """Review page shows confidence percentages for predictions."""
    url = live_server["url"]
    page.goto(f"{url}/review", timeout=5000)

    page.locator("[data-pred-id]").first.wait_for(state="visible", timeout=5000)

    confidence_elements = page.locator(".card-confidence")
    assert confidence_elements.count() >= 2
    # Check that confidence text contains a percentage
    first_conf = confidence_elements.first.text_content()
    assert "% confidence" in first_conf, f"Expected confidence text, got: {first_conf}"


def test_review_page_title_shows_pending_count(live_server, page):
    """Review page title includes the count of pending predictions."""
    url = live_server["url"]
    page.goto(f"{url}/review", timeout=5000)

    # Wait for predictions to load and title to update
    page.locator("[data-pred-id]").first.wait_for(state="visible", timeout=5000)

    title = page.locator("#title")
    expect(title).to_contain_text("pending")


def test_review_photo_size_slider_resizes_and_persists(live_server, page):
    """Review cards follow the photo-size control and retain its value."""
    url = live_server["url"]
    page.goto(f"{url}/review", timeout=5000)
    page.locator("[data-pred-id]").first.wait_for(state="visible", timeout=5000)

    slider = page.locator("#thumbSizeSlider")
    expect(slider).to_be_visible()
    expect(slider).to_have_value("400")
    expect(page.locator("#thumbSizeVal")).to_have_text("400px")
    initial_width = page.locator("[data-pred-id]").first.bounding_box()["width"]

    slider.evaluate(
        """el => {
            el.value = '240';
            el.dispatchEvent(new Event('input', { bubbles: true }));
        }"""
    )

    expect(page.locator("#thumbSizeVal")).to_have_text("240px")
    assert page.locator("#grid").evaluate(
        "el => el.style.getPropertyValue('--card-width')"
    ) == "240px"
    assert page.locator("[data-pred-id]").first.bounding_box()["width"] < initial_width

    page.reload()
    page.locator("[data-pred-id]").first.wait_for(state="visible", timeout=5000)
    expect(slider).to_have_value("240")
    expect(page.locator("#thumbSizeVal")).to_have_text("240px")
    assert page.locator("#grid").evaluate(
        "el => el.style.getPropertyValue('--card-width')"
    ) == "240px"


def test_review_sort_persists_across_navigation(live_server, page):
    url = live_server["url"]
    page.goto(f"{url}/review", timeout=5000)
    page.locator("[data-pred-id]").first.wait_for(state="visible", timeout=5000)

    page.locator("#sortSelect").select_option("confidence_asc")
    page.goto(f"{url}/")
    page.goto(f"{url}/review", timeout=5000)
    page.locator("[data-pred-id]").first.wait_for(state="visible", timeout=5000)

    expect(page.locator("#sortSelect")).to_have_value("confidence_asc")


def test_history_undo_refreshes_review_prediction_state(live_server, page):
    """Undo from the shared History panel must refresh Review's local state."""
    url = live_server["url"]
    page.goto(f"{url}/review", timeout=5000)

    card = page.locator(".card[data-pred-id]").first
    card.wait_for(state="visible", timeout=5000)
    pred_id = card.get_attribute("data-pred-id")

    with page.expect_response(
        lambda response: (
            f"/api/predictions/{pred_id}/accept" in response.url
            and response.status == 200
        )
    ):
        card.locator(".btn-accept").click()
    expect(card).to_have_class(re.compile(r"\baccepted\b"))

    with page.expect_response(
        lambda response: (
            response.url.endswith("/api/predictions")
            and response.status == 200
        )
    ):
        page.evaluate("window.doUndo()")

    expect(card).not_to_have_class(re.compile(r"\baccepted\b"))
    expect(card.locator(".btn-accept")).to_be_visible()


def test_review_photo_deep_link_pill_sits_under_controls_and_clears(live_server, page):
    """``/review?photo_id=N`` (Browse's ambiguous-prediction handoff) narrows
    the queue to one photo under a "Showing one photo from Browse" pill.

    The pill once anchored on a ``.toolbar`` Review doesn't have, so it was
    appended to the end of ``<body>``, where it sat behind the bottom-panel
    toggle and a real click on "show all ×" never reached it: the user was
    stuck in the one-photo view. It must sit between the action bar and the
    grid, and a real (unforced) click must bring the full queue back.
    """
    url = live_server["url"]
    photo_id = live_server["data"]["photos"][0]
    page.goto(f"{url}/review?photo_id={photo_id}", timeout=5000)

    cards = page.locator(".card[data-pred-id]")
    cards.first.wait_for(state="visible", timeout=5000)
    expect(cards).to_have_count(1)

    pill = page.locator("#photoFilterPill")
    expect(pill).to_be_visible()
    expect(pill).to_contain_text("Showing one photo from Browse")

    bar_box = page.locator("#reviewBar").bounding_box()
    pill_box = pill.bounding_box()
    grid_box = page.locator("#grid").bounding_box()
    assert bar_box["y"] + bar_box["height"] <= pill_box["y"] + 1, (
        f"pill {pill_box} should sit below the action bar {bar_box}"
    )
    assert pill_box["y"] + pill_box["height"] <= grid_box["y"] + 1, (
        f"pill {pill_box} should sit above the grid {grid_box}, not after it"
    )

    page.locator("#photoFilterClear").click()

    expect(pill).to_have_count(0)
    expect(cards).to_have_count(len(live_server["data"]["photos"]))
    assert "photo_id" not in page.url


@pytest.mark.parametrize("fingerprint", [
    "normal-fingerprint-that-is-truncated",
    "<img src=x>",
    "&lt;b&gt;",
])
def test_review_fingerprint_deep_link_is_literal_text_and_clears(
    live_server, page, fingerprint,
):
    """URL-supplied fingerprints are text, and clearing keeps the photo scope."""
    photo_id = live_server["data"]["photos"][0]
    page.goto(
        f"{live_server['url']}/review?photo_id={photo_id}"
        f"&labels_fingerprint={quote(fingerprint, safe='')}",
        timeout=5000,
    )

    pill = page.locator("#fpFilterPill")
    expect(pill).to_be_visible()
    expect(pill.locator("code")).to_have_text(fingerprint[:12])
    expect(pill.locator("code > *")).to_have_count(0)
    assert pill.evaluate("el => el.parentElement.id") == "reviewFilterPills"

    page.locator("#fpFilterClear").click()

    expect(pill).to_have_count(0)
    assert "labels_fingerprint" not in page.url
    assert f"photo_id={photo_id}" in page.url
    assert page.evaluate("currentLabelsFingerprint") is None
    expect(page.locator(".card[data-pred-id]")).to_have_count(1)
