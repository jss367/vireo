"""Scenario: visit the keywords management page.

Verifies that /keywords renders the keyword table with seeded data,
filter pills are present and clickable, and the search input exists.
"""


def run(session):
    session.goto("/keywords")

    # Wait for the keywords to load via /api/keywords/all
    session.page.wait_for_timeout(1000)

    session.screenshot("keywords-initial")

    # The search input should be present
    has_search = session.eval("!!document.getElementById('kwSearch')")
    session.assert_that(has_search, "expected keyword search input")

    # Filter pills should exist: All, General, Taxonomy, Location, Descriptive, People, Event
    filter_btns = session.eval(
        "Array.from(document.querySelectorAll('.kw-filter-btn')).map(b => b.dataset.type)"
    )
    expected_types = ["all", "general", "taxonomy", "location", "descriptive", "people", "event"]
    for t in expected_types:
        session.assert_that(
            t in filter_btns,
            f"expected filter button with data-type='{t}', got {filter_btns!r}",
        )

    # The "All" filter should be active by default
    active_filter = session.eval(
        """(() => {
            const btn = document.querySelector('.kw-filter-btn.active');
            return btn ? btn.dataset.type : null;
        })()"""
    )
    session.assert_that(active_filter == "all", f"expected 'all' filter active, got {active_filter!r}")

    # The keyword table should have rows (browse_seed has 4 keywords)
    row_count = session.eval(
        "document.querySelectorAll('#kwBody tr').length"
    )
    session.assert_that(row_count >= 4, f"expected at least 4 keyword rows, got {row_count}")

    # Stats should show keyword count
    stats_text = session.eval(
        "document.getElementById('kwStats').textContent"
    )
    session.assert_that(
        "keywords" in stats_text.lower() or "of" in stats_text.lower(),
        f"expected stats text to contain keyword count info, got {stats_text!r}",
    )

    # Click the Taxonomy filter and verify it becomes active
    session.click('.kw-filter-btn[data-type="taxonomy"]')
    session.page.wait_for_timeout(300)

    active_after = session.eval(
        """(() => {
            const btn = document.querySelector('.kw-filter-btn.active');
            return btn ? btn.dataset.type : null;
        })()"""
    )
    session.assert_that(
        active_after == "taxonomy",
        f"expected 'taxonomy' filter active after click, got {active_after!r}",
    )

    session.screenshot("keywords-taxonomy-filter")

    # Click "All" to restore full view
    session.click('.kw-filter-btn[data-type="all"]')
    session.page.wait_for_timeout(300)

    session.screenshot("keywords-all-filter")
