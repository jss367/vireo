"""Preview measurements follow the selected photo and the pixels on screen."""

from io import BytesIO
from pathlib import Path
from random import Random

import pytest
from PIL import Image
from playwright.sync_api import expect


def png(color):
    output = BytesIO()
    Image.new("RGB", (100, 100), color).save(output, format="PNG")
    return output.getvalue()


def open_photo(live_server, page, color="black"):
    page.route("**/thumbnails/*.jpg*", lambda route: route.fulfill(
        content_type="image/png", body=png(color),
    ))
    page.goto(live_server["url"] + "/browse")
    page.locator("#grid .grid-card").first.click()
    expect(page.locator("#browseHistogram")).to_be_visible()


@pytest.mark.parametrize("color,low,high", [
    ("black", "~100.0%", "~0.00%"),
    ("white", "~0.00%", "~100.0%"),
    ("red", "~0.00%", "~100.0%"),
    ((128, 128, 128), "~0.00%", "~0.00%"),
])
def test_histogram_measures_displayed_preview(live_server, page, color, low, high):
    open_photo(live_server, page, color)
    expect(page.locator("#browseHistogramLow")).to_have_text("Near black " + low)
    expect(page.locator("#browseHistogramHigh")).to_have_text("Near channel limit " + high)
    expect(page.locator("#browseHistogramStatus")).to_have_text("Sampled from the displayed preview.")
    # The graph is actually drawn, rather than only filling the text readouts.
    assert page.locator("#browseHistogram").evaluate(
        "canvas => canvas.getContext('2d').getImageData(0, 0, 256, 82).data.some(v => v > 0)"
    )


def test_histogram_channel_bins_and_thresholds(live_server, page):
    open_photo(live_server, page)
    result = page.evaluate("""() => pageModules.histogram.measureHistogram(new Uint8ClampedArray([
        0, 0, 0, 255, 3, 3, 3, 255, 4, 4, 4, 255,
        252, 0, 0, 255, 253, 0, 0, 255, 0, 255, 0, 255,
        255, 255, 255, 0
    ]))""")
    assert (result["low"], result["high"], result["total"]) == (2, 2, 6)
    red, green, blue = result["channels"]
    assert (red[0], red[1], red[63]) == (3, 1, 2)
    assert (green[0], green[1], green[63]) == (4, 1, 1)
    assert (blue[0], blue[1], blue[63]) == (5, 1, 0)


def test_histogram_updates_after_preview_source_change(live_server, page):
    open_photo(live_server, page)
    expect(page.locator("#browseHistogramLow")).to_have_text("Near black ~100.0%")
    # Lightbox edits and paired-source switches replace detailImg's src too.
    page.route("**/edited-preview.png", lambda route: route.fulfill(
        content_type="image/png", body=png("white"),
    ))
    page.evaluate("document.getElementById('detailImg').src = '/edited-preview.png'")
    expect(page.locator("#browseHistogramHigh")).to_have_text("Near channel limit ~100.0%")
    expect(page.locator("#browseHistogramLow")).to_have_text("Near black ~0.00%")


def test_failed_preview_clears_old_measurements_and_recovers(live_server, page):
    open_photo(live_server, page)
    page.route("**/missing-preview.png", lambda route: route.fulfill(status=404))
    page.evaluate("document.getElementById('detailImg').src = '/missing-preview.png'")
    expect(page.locator("#browseHistogramStatus")).to_have_text("Preview histogram unavailable.")
    expect(page.locator("#browseHistogram")).to_be_hidden()
    expect(page.locator("#browseHistogramValues")).to_be_hidden()
    page.route("**/recovered-preview.png", lambda route: route.fulfill(
        content_type="image/png", body=png("white"),
    ))
    page.evaluate("document.getElementById('detailImg').src = '/recovered-preview.png'")
    expect(page.locator("#browseHistogram")).to_be_visible()
    expect(page.locator("#browseHistogramHigh")).to_have_text("Near channel limit ~100.0%")


def test_preview_loading_hides_previous_measurements(live_server, page):
    open_photo(live_server, page)
    held = []
    page.route("**/delayed-preview.png", lambda route: held.append(route))
    with page.expect_request("**/delayed-preview.png"):
        page.evaluate("document.getElementById('detailImg').src = '/delayed-preview.png'")
    expect(page.locator("#browseHistogramStatus")).to_have_text("Loading preview…")
    expect(page.locator("#browseHistogram")).to_be_hidden()
    expect(page.locator("#browseHistogramValues")).to_be_hidden()
    held[0].fulfill(content_type="image/png", body=png("white"))
    expect(page.locator("#browseHistogram")).to_be_visible()
    expect(page.locator("#browseHistogramHigh")).to_have_text("Near channel limit ~100.0%")


def test_selection_change_clears_histogram_while_detail_is_loading(live_server, page):
    open_photo(live_server, page)
    held = []
    page.route("**/api/photos/*", lambda route: held.append(route))
    with page.expect_request("**/api/photos/*"):
        page.locator("#grid .grid-card").nth(1).click()
    expect(page.locator("#browseHistogram")).to_be_hidden()
    expect(page.locator("#browseHistogramValues")).to_be_hidden()
    # An old preview load cannot revive the previous photo's measurements.
    page.evaluate("document.getElementById('detailImg').dispatchEvent(new Event('load'))")
    expect(page.locator("#browseHistogram")).to_be_hidden()
    held[0].continue_()
    expect(page.locator("#browseHistogram")).to_be_visible()


def test_histogram_hidden_for_batch_and_restored_for_single(live_server, page):
    open_photo(live_server, page)
    page.locator("#grid .grid-card").nth(1).click(modifiers=["ControlOrMeta"])
    expect(page.locator("#browseHistogramSection")).to_be_hidden()
    page.locator("#grid .grid-card").nth(2).click()
    expect(page.locator("#browseHistogram")).to_be_visible()
    expect(page.locator("#browseHistogramLow")).to_have_text("Near black ~100.0%")


def test_histogram_visual_preview(live_server, page):
    # A repeatable spread of colors makes channel overlap inspectable.
    image = Image.new("RGB", (256, 160))
    random = Random(97)
    image.putdata([tuple(max(0, min(255, round(random.gauss(mean, 38))))
                         for mean in (100, 140, 170)) for _ in range(256 * 160)])
    output = BytesIO()
    image.save(output, format="PNG")
    page.route("**/thumbnails/*.jpg*", lambda route: route.fulfill(
        content_type="image/png", body=output.getvalue(),
    ))
    page.set_viewport_size({"width": 1440, "height": 1000})
    page.goto(live_server["url"] + "/browse")
    page.locator("#grid .grid-card").first.click()
    expect(page.locator("#browseHistogram")).to_be_visible()
    for element in ("#browseHistogramLow", "#browseHistogramHigh", "#browseHistogramStatus"):
        expect(page.locator(element)).to_be_visible()
    assert page.locator("#browseHistogramSection").evaluate(
        "section => section.scrollWidth <= section.clientWidth"
    )
    destination = Path(__file__).resolve().parents[2] / ".context" / "browse-histogram.png"
    destination.parent.mkdir(exist_ok=True)
    page.screenshot(path=str(destination))
