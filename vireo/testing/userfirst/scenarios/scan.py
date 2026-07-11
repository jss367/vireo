"""Scenario: visit the pipeline (process) page.

Verifies that /pipeline renders, the stage cards are present (Source,
Scan & Index, etc.), and the "Start Pipeline" button exists. We do not
trigger an actual scan because that requires real photo files and ML models.
"""


def run(session):
    session.goto("/pipeline")
    session.screenshot("pipeline-initial")

    # Stage cards should be present (at least Source, Scan & Index, Previews)
    stage_count = session.eval(
        "document.querySelectorAll('.stage-card').length"
    )
    session.assert_that(stage_count >= 3, f"expected at least 3 stage cards, got {stage_count}")

    # Stage names should include Source and Scan & Index. Destination was
    # removed in the import/process split — copying files is Import's job.
    stage_names = session.eval(
        """Array.from(document.querySelectorAll('.stage-name')).map(el => el.textContent.trim())"""
    )
    session.assert_that(
        "Source" in stage_names,
        f"expected 'Source' in stage names, got {stage_names!r}",
    )
    session.assert_that(
        "Destination" not in stage_names,
        f"'Destination' should be gone from the process page, got {stage_names!r}",
    )
    session.assert_that(
        "Scan & Index" in stage_names,
        f"expected 'Scan & Index' in stage names, got {stage_names!r}",
    )

    # The "Start Pipeline" button should exist
    has_start_btn = session.eval("!!document.getElementById('btnStartPipeline')")
    session.assert_that(has_start_btn, "expected Start Pipeline button")

    # The Source card offers the Folders scope (importing moved to /import
    # in the import/process split).
    has_folders_radio = session.eval("!!document.getElementById('radioFolders')")
    session.assert_that(has_folders_radio, "expected Folders radio button in Source card")

    session.screenshot("pipeline-loaded")
