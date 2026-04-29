import re

from playwright.sync_api import expect


def test_pipeline_page_loads_with_stages(live_server, page):
    url = live_server["url"]
    page.goto(f"{url}/pipeline")
    stages = page.locator("[data-testid='stage-card']")
    expect(stages).to_have_count(8)


def test_pipeline_start_button_disabled_without_folders(live_server, page):
    url = live_server["url"]
    page.goto(f"{url}/pipeline")
    btn = page.locator("[data-testid='start-pipeline-btn']")
    expect(btn).to_be_disabled()


def test_pipeline_copy_toggle_shows_destination(live_server, page):
    url = live_server["url"]
    page.goto(f"{url}/pipeline")
    # Expand the Destination card first (collapsed by default)
    page.click("#card-destination .stage-header")
    dest = page.locator("[data-testid='destination-section']")
    expect(dest).to_be_hidden()
    page.check("[data-testid='copy-photos-toggle']")
    expect(dest).to_be_visible()


def test_pipeline_copy_toggle_hides_destination(live_server, page):
    url = live_server["url"]
    page.goto(f"{url}/pipeline")
    page.click("#card-destination .stage-header")
    page.check("[data-testid='copy-photos-toggle']")
    dest = page.locator("[data-testid='destination-section']")
    expect(dest).to_be_visible()
    page.uncheck("[data-testid='copy-photos-toggle']")
    expect(dest).to_be_hidden()


def test_pipeline_collection_source_dims_import(live_server, page):
    url = live_server["url"]
    page.goto(f"{url}/pipeline")
    page.click("[data-testid='source-collection']")
    import_body = page.locator("#sourceImportBody")
    expect(import_body).to_have_class(re.compile("dimmed"))


def test_pipeline_import_source_dims_collection(live_server, page):
    url = live_server["url"]
    page.goto(f"{url}/pipeline")
    page.click("[data-testid='source-collection']")
    collection_body = page.locator("[data-testid='collection-section']")
    expect(collection_body).not_to_have_class(re.compile("dimmed"))
    page.click("[data-testid='source-import']")
    expect(collection_body).to_have_class(re.compile("dimmed"))


def test_pipeline_source_card_expanded_by_default(live_server, page):
    url = live_server["url"]
    page.goto(f"{url}/pipeline")
    source_card = page.locator("#card-source")
    expect(source_card).to_have_class(re.compile("expanded"))


def test_pipeline_stage_cards_collapse_expand(live_server, page):
    url = live_server["url"]
    page.goto(f"{url}/pipeline")
    page.click("#card-source .stage-header")
    source_card = page.locator("#card-source")
    expect(source_card).not_to_have_class(re.compile("expanded"))
    page.click("#card-source .stage-header")
    expect(source_card).to_have_class(re.compile("expanded"))


def test_pipeline_folder_template_visible_when_copy_enabled(live_server, page):
    url = live_server["url"]
    page.goto(f"{url}/pipeline")
    page.click("#card-destination .stage-header")
    page.check("[data-testid='copy-photos-toggle']")
    template = page.locator("#cfgFolderTemplate")
    expect(template).to_be_visible()


def test_pipeline_folder_template_hidden_when_copy_disabled(live_server, page):
    url = live_server["url"]
    page.goto(f"{url}/pipeline")
    page.click("#card-destination .stage-header")
    # Don't check the copy toggle
    template = page.locator("#cfgFolderTemplate")
    expect(template).to_be_hidden()


def test_pipeline_custom_template_shown_on_select(live_server, page):
    url = live_server["url"]
    page.goto(f"{url}/pipeline")
    page.click("#card-destination .stage-header")
    page.check("[data-testid='copy-photos-toggle']")
    custom_input = page.locator("[data-testid='custom-template-input']")
    expect(custom_input).to_be_hidden()
    page.select_option("#cfgFolderTemplate", "__custom__")
    expect(custom_input).to_be_visible()


def test_pipeline_preview_button_disabled_without_source_dest(live_server, page):
    url = live_server["url"]
    page.goto(f"{url}/pipeline")
    page.click("#card-destination .stage-header")
    page.check("[data-testid='copy-photos-toggle']")
    btn = page.locator("[data-testid='preview-folders-btn']")
    expect(btn).to_be_disabled()


def test_pipeline_section_headers_visible(live_server, page):
    url = live_server["url"]
    page.goto(f"{url}/pipeline")
    headers = page.locator(".stage-section-header")
    expect(headers).to_have_count(3)
    expect(headers.nth(0)).to_contain_text("Setup")
    expect(headers.nth(1)).to_contain_text("Indexing")
    expect(headers.nth(2)).to_contain_text("AI processing")


def test_pipeline_status_pills_visible_for_processing_stages(live_server, page):
    url = live_server["url"]
    page.goto(f"{url}/pipeline")
    # Stages 3-8 each have a pill that's populated on load.
    for suffix in ["Scan", "Previews", "Classify", "Extract", "EyeKeypoints", "Group"]:
        pill = page.locator(f"#pill{suffix}")
        expect(pill).to_be_visible()
    # Indexing stages always start at "Will run".
    expect(page.locator("#pillScan")).to_contain_text("Will run")
    # Seeded fixture has detections → Classify shows "Already done".
    expect(page.locator("#pillClassify")).to_contain_text("Already done")
    # Extract has no seeded masks → "Will run".
    expect(page.locator("#pillExtract")).to_contain_text("Will run")


def test_pipeline_reclassify_flips_classify_pill_to_will_run(live_server, page):
    url = live_server["url"]
    page.goto(f"{url}/pipeline")
    expect(page.locator("#pillClassify")).to_contain_text("Already done")
    page.click("#card-classify .stage-header")
    page.check("#chkReclassify")
    expect(page.locator("#pillClassify")).to_contain_text("Will run")


def test_pipeline_toggling_classify_off_marks_downstream_will_skip(live_server, page):
    url = live_server["url"]
    page.goto(f"{url}/pipeline")
    page.click("#card-classify .stage-header")
    page.uncheck("#enableClassify")
    for suffix in ["Classify", "Extract", "Group"]:
        pill = page.locator(f"#pill{suffix}")
        expect(pill).to_contain_text("Will skip")
    # Indexing stages are unaffected.
    expect(page.locator("#pillScan")).to_contain_text("Will run")


def test_pipeline_plan_summary_lists_stages(live_server, page):
    url = live_server["url"]
    page.goto(f"{url}/pipeline")
    summary = page.locator("[data-testid='pipeline-plan-summary']")
    expect(summary).to_be_visible()
    will_run = summary.locator(".plan-row.will-run .plan-stages")
    # Extract & Group will run; Classify will not (Already done from seed).
    expect(will_run).to_contain_text("Extract Features")
    expect(will_run).to_contain_text("Group & Score")
    done_prior = summary.locator(".plan-row.done-prior .plan-stages")
    expect(done_prior).to_contain_text("Classify")


def test_pipeline_plan_summary_updates_on_toggle(live_server, page):
    url = live_server["url"]
    page.goto(f"{url}/pipeline")
    page.click("#card-classify .stage-header")
    page.uncheck("#enableClassify")
    summary = page.locator("[data-testid='pipeline-plan-summary']")
    skip_row = summary.locator(".plan-row.will-skip .plan-stages")
    expect(skip_row).to_contain_text("Classify")
    expect(skip_row).to_contain_text("Extract Features")
    expect(skip_row).to_contain_text("Group & Score")


def test_pipeline_concurrent_running_stages_keep_running_pill(live_server, page):
    """Multiple concurrent running stages must all keep the Running pill, even
    after refreshPipelineUI() is invoked (e.g. via a toggle change).

    Regression test for the single-string `_runningStageSuffix` bug: when the
    pipeline ran scan + thumbnails in parallel, each running event clobbered
    the slot, leaving only one stage pulsing while the other was recomputed
    back to "Will run" on the next refresh.
    """
    url = live_server["url"]
    page.goto(f"{url}/pipeline")
    page.evaluate("""
        _updatePipelineStageUI({stages: {
            scan: {status: 'running'},
            thumbnails: {status: 'running'},
        }});
    """)
    expect(page.locator("#pillScan")).to_contain_text("Running")
    expect(page.locator("#pillPreviews")).to_contain_text("Running")
    # Triggering a recompute must NOT flip still-running stages back.
    page.evaluate("refreshPipelineUI();")
    expect(page.locator("#pillScan")).to_contain_text("Running")
    expect(page.locator("#pillPreviews")).to_contain_text("Running")


def test_pipeline_skipped_user_disabled_stage_keeps_will_skip_pill(live_server, page):
    """When the user disables a stage and the backend reports it `skipped`,
    the pill must stay "Will skip" — not flip to "Already done"."""
    url = live_server["url"]
    page.goto(f"{url}/pipeline")
    page.click("#card-classify .stage-header")
    page.uncheck("#enableExtract")
    expect(page.locator("#pillExtract")).to_contain_text("Will skip")
    # Simulate the backend `skipped` status the orchestrator emits for a
    # user-disabled stage (params.skip_extract_masks branch in pipeline_job).
    page.evaluate("""
        _updatePipelineStageUI({stages: {extract_masks: {status: 'skipped'}}});
    """)
    expect(page.locator("#pillExtract")).to_contain_text("Will skip")
    expect(page.locator("#pillExtract")).not_to_contain_text("Already done")


def test_pipeline_skipped_auto_stage_shows_already_done(live_server, page):
    """When the backend reports `skipped` for a stage the user *did not*
    disable (e.g. nothing eligible to process), the pill should show
    "Already done" — distinguishing it from an explicit user skip."""
    url = live_server["url"]
    page.goto(f"{url}/pipeline")
    # Extract is enabled by default.
    expect(page.locator("#pillExtract")).to_contain_text("Will run")
    page.evaluate("""
        _updatePipelineStageUI({stages: {extract_masks: {status: 'skipped'}}});
    """)
    expect(page.locator("#pillExtract")).to_contain_text("Already done")


def test_pipeline_eye_keypoints_pill_will_skip_when_no_weights(live_server, page):
    """Eye Keypoints has no enable checkbox — it's gated by whether
    SuperAnimal weights are on disk. The fixture doesn't ship any keypoint
    models, so /api/models/keypoints/status reports both as 'missing' and
    the pill must show 'Will skip' (mirroring the backend preflight),
    not 'Will run'.
    """
    url = live_server["url"]
    page.goto(f"{url}/pipeline")
    expect(page.locator("#pillEyeKeypoints")).to_contain_text("Will skip")
    summary = page.locator("[data-testid='pipeline-plan-summary']")
    skip_row = summary.locator(".plan-row.will-skip .plan-stages")
    expect(skip_row).to_contain_text("Eye Keypoints")
    will_run_row = summary.locator(".plan-row.will-run .plan-stages")
    expect(will_run_row).not_to_contain_text("Eye Keypoints")


def test_pipeline_eye_keypoints_pill_will_run_when_models_ready(live_server, page):
    """When at least one keypoint model is ready, the pill flips back to
    'Will run'. Simulated by stubbing the status endpoint client-side and
    re-invoking refreshKeypointsStatus."""
    url = live_server["url"]
    page.goto(f"{url}/pipeline")
    expect(page.locator("#pillEyeKeypoints")).to_be_visible()
    page.evaluate("""
        window._keypointModelsReady = true;
        refreshPipelineUI();
    """)
    expect(page.locator("#pillEyeKeypoints")).to_contain_text("Will run")
