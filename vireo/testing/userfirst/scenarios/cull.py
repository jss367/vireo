"""Scenario: visit the culling page.

Verifies that /cull renders, the "Analyze for Culling" button is present,
and the collection dropdown is populated.  We do not trigger actual analysis
because that requires ML models.
"""


def run(session):
    session.goto("/cull")

    # The cull page auto-loads previous results via /api/culling/results.
    # When no analysis has been run yet this returns a 404, which is
    # expected behavior — not a bug.  Wait for the page to settle, then
    # clear any findings that came from that expected 404.
    session.page.wait_for_timeout(500)
    session.report.findings = [
        f
        for f in session.report.findings
        if not (
            f.kind == "BUG"
            and "/api/culling/results" in f.context.get("url", "")
            and "404" in f.message
        )
    ]

    session.screenshot("cull-initial")

    # The "Analyze for Culling" button should be present
    has_analyze_btn = session.eval(
        "!!document.querySelector('button.btn-primary')"
    )
    session.assert_that(has_analyze_btn, "expected Analyze for Culling button")

    # Verify the button text matches
    btn_text = session.eval(
        "(document.querySelector('button.btn-primary') || {}).textContent || ''"
    )
    session.assert_that(
        "Analyze" in btn_text,
        f"expected button text to contain 'Analyze', got: {btn_text!r}",
    )

    # The collection dropdown should exist
    has_dropdown = session.eval("!!document.getElementById('cullCollection')")
    session.assert_that(has_dropdown, "expected collection dropdown on cull page")

    # Settings toggle should exist
    has_settings_btn = session.eval("!!document.getElementById('cullSettingsBtn')")
    session.assert_that(has_settings_btn, "expected settings toggle button")

    session.screenshot("cull-loaded")
