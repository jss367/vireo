"""Scenario: visit the duplicates detection page.

Verifies that /duplicates renders, the "Scan for duplicate files" button
is present, and the empty state is shown when no duplicates have been
scanned.
"""


def run(session):
    session.goto("/duplicates")
    session.screenshot("duplicates-initial")

    # The page header should contain "Duplicate Files"
    header_text = session.eval(
        """(() => {
            const h1 = document.querySelector('.page-header h1');
            return h1 ? h1.textContent.trim() : '';
        })()"""
    )
    session.assert_that(
        "Duplicate" in header_text,
        f"expected 'Duplicate' in page header, got {header_text!r}",
    )

    # The "Scan for duplicate files" button should exist
    has_scan_btn = session.eval("!!document.getElementById('scanBtn')")
    session.assert_that(has_scan_btn, "expected scan button on duplicates page")

    # Verify scan button text
    scan_btn_text = session.eval(
        "(document.getElementById('scanBtn') || {}).textContent || ''"
    )
    session.assert_that(
        "Scan" in scan_btn_text,
        f"expected 'Scan' in button text, got {scan_btn_text!r}",
    )

    # The empty state should be visible (no scan has been performed)
    empty_visible = session.eval(
        """(() => {
            const el = document.getElementById('emptyState');
            return el ? el.style.display !== 'none' : false;
        })()"""
    )
    session.assert_that(empty_visible, "expected empty state visible before scan")

    # The progress box should be hidden
    progress_hidden = session.eval(
        """(() => {
            const el = document.getElementById('progress');
            return el ? el.style.display === 'none' : true;
        })()"""
    )
    session.assert_that(progress_hidden, "expected progress box hidden before scan")

    # The apply bar should be hidden (no results to apply)
    apply_hidden = session.eval(
        """(() => {
            const el = document.getElementById('applyBar');
            return el ? el.style.display === 'none' : true;
        })()"""
    )
    session.assert_that(apply_hidden, "expected apply bar hidden before scan")

    session.screenshot("duplicates-empty-state")
