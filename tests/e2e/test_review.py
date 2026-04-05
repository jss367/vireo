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
