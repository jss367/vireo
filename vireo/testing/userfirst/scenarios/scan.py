"""Scenario: visit the pipeline (scan/import) page.

Verifies that /pipeline renders, the stage cards are present (Source,
Destination, Scan & Import, etc.), and the "Start Pipeline" button exists.
We do not trigger an actual scan because that requires real photo files and
ML models.
"""


def run(session):
    session.goto("/pipeline")
    session.screenshot("pipeline-initial")

    # Stage cards should be present (at least the first 3: Source, Destination, Scan & Import)
    stage_count = session.eval(
        "document.querySelectorAll('.stage-card').length"
    )
    session.assert_that(stage_count >= 3, f"expected at least 3 stage cards, got {stage_count}")

    # Stage names should include Source, Destination, Scan & Import
    stage_names = session.eval(
        """Array.from(document.querySelectorAll('.stage-name')).map(el => el.textContent.trim())"""
    )
    session.assert_that(
        "Source" in stage_names,
        f"expected 'Source' in stage names, got {stage_names!r}",
    )
    session.assert_that(
        "Destination" in stage_names,
        f"expected 'Destination' in stage names, got {stage_names!r}",
    )
    session.assert_that(
        "Scan & Import" in stage_names,
        f"expected 'Scan & Import' in stage names, got {stage_names!r}",
    )

    # The "Start Pipeline" button should exist
    has_start_btn = session.eval("!!document.getElementById('btnStartPipeline')")
    session.assert_that(has_start_btn, "expected Start Pipeline button")

    # The Source card should have the Import radio option
    has_import_radio = session.eval("!!document.getElementById('radioImport')")
    session.assert_that(has_import_radio, "expected Import radio button in Source card")

    session.screenshot("pipeline-loaded")
