"""Scenario: visit the pipeline review page.

Verifies that /pipeline/review renders and shows the expected empty state
when no pipeline results exist. Also checks that the sidebar and filter
bar chrome are present.
"""

import contextlib


def run(session):
    session.goto("/pipeline/review")

    # Wait for initPipelineReviewPage()'s /api/pipeline/page-init fetch to
    # resolve rather than using a fixed sleep.  The callback either shows
    # #emptyState (no results) or populates #encountersContainer (results
    # present); either transition means init completed.  A fixed 1s wait
    # races the fetch on slow CI and produces false BUGs on the
    # empty-state assertion.  Fall through on timeout so the assertions
    # below surface the stale state rather than hang.
    with contextlib.suppress(Exception):
        session.page.wait_for_function(
            """() => {
                const empty = document.getElementById('emptyState');
                const cont = document.getElementById('encountersContainer');
                if (!empty || !cont) return false;
                return empty.style.display !== 'none' || cont.children.length > 0;
            }""",
            timeout=10000,
        )

    session.screenshot("pipeline-review-initial")

    # The page layout should have the pipeline sidebar
    has_sidebar = session.eval("!!document.querySelector('.pipeline-sidebar')")
    session.assert_that(has_sidebar, "expected pipeline sidebar")

    # The pipeline main content area should exist
    has_main = session.eval("!!document.querySelector('.pipeline-main')")
    session.assert_that(has_main, "expected pipeline main area")

    # Without pipeline results, the empty state should be visible
    empty_visible = session.eval(
        """(() => {
            const el = document.getElementById('emptyState');
            return el ? el.style.display !== 'none' : false;
        })()"""
    )
    session.assert_that(empty_visible, "expected empty state visible when no pipeline results")

    # The empty state should contain a link to /pipeline
    empty_link = session.eval(
        """(() => {
            const el = document.getElementById('emptyState');
            if (!el) return false;
            const a = el.querySelector('a[href="/pipeline"]');
            return !!a;
        })()"""
    )
    session.assert_that(empty_link, "expected link to /pipeline in empty state")

    # The filter bar should be present (even if no results)
    has_filter_bar = session.eval("!!document.querySelector('.filter-bar')")
    session.assert_that(has_filter_bar, "expected filter bar on pipeline review page")

    # The encounters container should exist (empty)
    has_container = session.eval("!!document.getElementById('encountersContainer')")
    session.assert_that(has_container, "expected encounters container")

    session.screenshot("pipeline-review-empty")
