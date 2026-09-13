from playwright.sync_api import expect


def test_right_click_color_adds_workspace_description(live_server, page):
    """A color dot opens the description editor and the saved text becomes its tooltip."""
    page.goto(f"{live_server['url']}/browse")
    first = page.locator(".grid-card").first
    first.wait_for(state="visible")
    first.click()

    red = page.locator('#detailColors [data-color="red"]')
    expect(red).to_be_visible()
    expect(red).to_have_attribute("title", "Red (6) · Right-click to add a description")
    red.click(button="right")

    modal = page.locator("#colorLabelDescriptionModal")
    expect(modal).to_have_class("modal-overlay open")
    expect(page.locator("#colorLabelDescriptionHeading")).to_have_text(
        "Red label description"
    )
    field = page.locator("#colorLabelDescriptionInput")
    field.fill("Reptiles")
    page.locator("#colorLabelDescriptionSave").click()

    expect(modal).not_to_have_class("modal-overlay open")
    expect(red).to_have_attribute(
        "title", "Red (6) — Reptiles · Right-click to edit"
    )
    expect(red).to_have_attribute("aria-label", "Red label: Reptiles")

    # The same workspace meaning follows the color into other color-label UI.
    quick_red = page.locator('.vf-quick-colors [data-color="red"]')
    expect(quick_red).to_have_attribute(
        "title", "Red label — Reptiles · Right-click to edit"
    )

    response = page.request.get(f"{live_server['url']}/api/color-label-descriptions")
    assert response.ok
    assert response.json() == {"red": "Reptiles"}


def test_lazily_rendered_color_badges_include_workspace_description(live_server, page):
    """Cards appended after the description loaded still get the described aria-label.

    Regression: `refreshControls()` fires only when descriptions load or are
    saved, so cards that go through `appendGridPhotos` afterwards used to keep
    the hardcoded "Red label" aria-label instead of "Red label: Reptiles".
    """
    url = live_server["url"]
    photos = live_server["data"]["photos"]

    put = page.request.put(
        f"{url}/api/color-label-descriptions/red",
        data={"description": "Reptiles"},
    )
    assert put.ok
    color_response = page.request.post(
        f"{url}/api/photos/{photos[0]}/color_label",
        data={"color": "red"},
    )
    assert color_response.ok

    page.goto(f"{url}/browse")
    page.locator(".grid-card").first.wait_for(state="visible")
    # Ensure descriptions have finished loading before we simulate a lazy append.
    page.wait_for_function(
        "window.VireoColorLabels"
        " && window.VireoColorLabels.description('red') === 'Reptiles'",
        timeout=3000,
    )

    # Simulate a lazy-loaded batch by re-appending the seeded photo through
    # the same code path the scroll loader uses. Before the fix the newly
    # inserted badge kept its build-time aria-label of "Red label".
    page.evaluate(
        """(pid) => {
            const original = photos.find(p => p.id === pid);
            if (!original) throw new Error('seed photo missing from page state');
            if (!cardFields.includes('color_label')) cardFields.push('color_label');
            const clone = Object.assign({}, original, { id: pid + 90000 });
            colorLabels[clone.id] = 'red';
            colorLabelsFetched.add(clone.id);
            appendGridPhotos([clone], 1000);
        }""",
        photos[0],
    )

    appended = page.locator(
        f'.grid-card[data-id="{photos[0] + 90000}"] .grid-card-color[data-color="red"]'
    )
    expect(appended).to_have_attribute("aria-label", "Red label: Reptiles")
    expect(appended).to_have_attribute(
        "title", "Red — Reptiles · Right-click to edit"
    )


def test_color_description_can_be_removed_and_right_click_does_not_filter(
    live_server, page
):
    """Opening or clearing the editor never toggles the clicked color filter."""
    page.request.put(
        f"{live_server['url']}/api/color-label-descriptions/blue",
        data={"description": "Waterbirds"},
    )
    page.goto(f"{live_server['url']}/browse")
    page.locator(".grid-card").first.wait_for(state="visible")
    blue = page.locator('.vf-quick-colors [data-color="blue"]')
    expect(blue).to_have_attribute(
        "title", "Blue label — Waterbirds · Right-click to edit"
    )

    page.click(".vf-filters-btn")
    blue.click(button="right")
    assert "active" not in (blue.get_attribute("class") or "").split()
    field = page.locator("#colorLabelDescriptionInput")
    expect(field).to_have_value("Waterbirds")
    field.fill("")
    page.locator("#colorLabelDescriptionSave").click()

    expect(blue).to_have_attribute(
        "title", "Blue label · Right-click to add a description"
    )
    assert page.request.get(
        f"{live_server['url']}/api/color-label-descriptions"
    ).json() == {}
