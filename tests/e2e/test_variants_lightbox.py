"""E2E coverage for lightbox navigation on the visual variants page."""

import base64

from playwright.sync_api import expect

_PNG_1X1 = (
    "iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAQAAAC1HAwCAAAAC0lEQVR42mP8"
    "/x8AAwMCAO+/p9sAAAAASUVORK5CYII="
)


def test_variants_lightbox_arrows_navigate_displayed_photos(live_server, page):
    """Variant thumbnails provide the shared lightbox its navigation list."""
    page.route(
        "**/photos/*/full*",
        lambda route: route.fulfill(
            body=base64.b64decode(_PNG_1X1), content_type="image/png"
        ),
    )
    page.goto(f"{live_server['url']}/variants")

    page.evaluate(
        """() => renderClusters({
            species: 'Test bird',
            total_photos: 2,
            num_clusters: 2,
            distance_threshold: 0.4,
            clusters: [
                {
                    count: 1,
                    photos: [{photo: {id: 900001, filename: 'first.jpg'}}]
                },
                {
                    count: 1,
                    photos: [{photo: {id: 900002, filename: 'second.jpg'}}]
                }
            ]
        })"""
    )

    page.locator('.cluster-thumb[data-photo-id="900001"]').click()
    expect(page.locator("#lightboxOverlay")).to_have_class("lightbox-overlay active")
    expect(page.locator("#lightboxFilename")).to_have_text("first.jpg")
    expect(page.locator("#lightboxCounter")).to_contain_text("1 / 2")

    page.locator("#lightboxNext").click()
    expect(page.locator("#lightboxFilename")).to_have_text("second.jpg")
    expect(page.locator("#lightboxCounter")).to_contain_text("2 / 2")

    page.locator("#lightboxPrev").click()
    expect(page.locator("#lightboxFilename")).to_have_text("first.jpg")
    expect(page.locator("#lightboxCounter")).to_contain_text("1 / 2")


def test_variants_lightbox_deduplicates_photos_across_clusters(live_server, page):
    """A photo that appears in more than one cluster still lets next/prev advance."""
    page.route(
        "**/photos/*/full*",
        lambda route: route.fulfill(
            body=base64.b64decode(_PNG_1X1), content_type="image/png"
        ),
    )
    page.goto(f"{live_server['url']}/variants")

    page.evaluate(
        """() => renderClusters({
            species: 'Test bird',
            total_photos: 3,
            num_clusters: 2,
            distance_threshold: 0.4,
            clusters: [
                {
                    count: 2,
                    photos: [
                        {photo: {id: 900010, filename: 'first.jpg'}},
                        {photo: {id: 900011, filename: 'second.jpg'}}
                    ]
                },
                {
                    count: 2,
                    photos: [
                        {photo: {id: 900011, filename: 'second.jpg'}},
                        {photo: {id: 900012, filename: 'third.jpg'}}
                    ]
                }
            ]
        })"""
    )

    # Opening the duplicated photo must land on its first (deduped) position
    # and allow forward navigation to reach the third photo.
    page.locator('.cluster-thumb[data-photo-id="900011"]').first.click()
    expect(page.locator("#lightboxOverlay")).to_have_class("lightbox-overlay active")
    expect(page.locator("#lightboxFilename")).to_have_text("second.jpg")
    expect(page.locator("#lightboxCounter")).to_contain_text("2 / 3")

    page.locator("#lightboxNext").click()
    expect(page.locator("#lightboxFilename")).to_have_text("third.jpg")
    expect(page.locator("#lightboxCounter")).to_contain_text("3 / 3")
