"""Scenario: browse the photo grid.

Verifies that /browse renders seeded photos in grid cards, the sidebar is
present, and the filter bar is visible.
"""


def run(session):
    session.goto("/browse")
    session.screenshot("browse-initial")

    # Assert photos appear in the grid
    photo_count = session.eval(
        "document.querySelectorAll('.grid-card[data-id]').length"
    )
    session.assert_that(photo_count > 0, "expected photos in browse grid")

    # Sidebar should be present with folder tree
    has_sidebar = session.eval("!!document.querySelector('.sidebar')")
    session.assert_that(has_sidebar, "expected sidebar on browse page")

    # Filter bar should be visible
    has_filter_bar = session.eval("!!document.querySelector('.filter-bar')")
    session.assert_that(has_filter_bar, "expected filter bar on browse page")

    # At least one photo should show a rating (stars)
    rated_count = session.eval(
        "document.querySelectorAll('.grid-card-rating').length"
    )
    session.assert_that(rated_count > 0, "expected at least one rated photo")

    session.screenshot("browse-populated")
