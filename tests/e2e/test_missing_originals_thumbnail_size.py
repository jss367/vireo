from playwright.sync_api import expect


def test_missing_originals_thumbnail_slider_resizes_previews(live_server, page):
    page.goto(f"{live_server['url']}/browse")

    slider = page.locator("#missingPhotosThumbSizeSlider")
    expect(slider).to_have_value("56")

    page.evaluate(
        """() => {
            const list = document.getElementById('missingPhotosList');
            list.innerHTML = `
                <div class="missing-photo-row">
                    <div class="missing-photo-thumb"><span>preview</span></div>
                </div>`;
        }"""
    )
    thumb = page.locator(".missing-photo-thumb")
    expect(thumb).to_have_css("width", "56px")
    expect(thumb).to_have_css("height", "56px")

    slider.evaluate(
        """el => {
            el.value = '120';
            el.dispatchEvent(new Event('input', { bubbles: true }));
        }"""
    )

    expect(thumb).to_have_css("width", "120px")
    expect(thumb).to_have_css("height", "120px")
