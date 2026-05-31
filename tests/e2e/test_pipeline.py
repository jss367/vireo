import json
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


def test_pipeline_import_plan_waits_for_folder_preview_scope(live_server, page):
    url = live_server["url"]
    page.goto(f"{url}/pipeline")
    expect(page.locator("#pillClassify")).to_contain_text("Already done")

    stale_plan_route = []

    def hold_stale_plan(route):
        stale_plan_route.append(route)

    page.route("**/api/pipeline/plan", hold_stale_plan)
    page.evaluate("setTimeout(refreshPipelinePlan, 0)")
    for _ in range(50):
        if stale_plan_route:
            break
        page.wait_for_timeout(100)
    assert stale_plan_route

    folder_preview_route = []
    page.route("**/api/import/folder-preview", lambda route: folder_preview_route.append(route))
    page.fill("#cfgSourceInput", "/Volumes/Photography/Raw Files/USA/2026/2026-05-30")
    page.locator("#card-source button", has_text="Add").click()
    stale_plan_route[0].fulfill(
        status=200,
        content_type="application/json",
        body=json.dumps({
            "stages": {
                "Previews": {"state": "done-prior", "summary": "stale"},
                "Classify": {"state": "done-prior", "summary": "stale"},
                "Extract": {"state": "done-prior", "summary": "stale"},
                "EyeKeypoints": {"state": "done-prior", "summary": "stale"},
                "Group": {"state": "done-prior", "summary": "stale"},
            },
            "scope": {"collection_id": None, "photo_count": None, "new_count": 0, "known_count": 0},
        }),
    )

    expect(page.locator("[data-testid='pipeline-plan-summary'] .plan-loading")).to_be_visible()
    expect(page.locator("#pillClassify")).not_to_contain_text("Already done")
    for _ in range(50):
        if folder_preview_route:
            break
        page.wait_for_timeout(100)
    assert folder_preview_route
    folder_preview_route[0].fulfill(
        status=200,
        content_type="application/json",
        body=json.dumps({
            "total_count": 0,
            "total_size": 0,
            "type_breakdown": {},
            "duplicate_count": 0,
            "files": [],
        }),
    )


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
    # Extract & Group will run; Classify is done-prior (seed has classifier_runs).
    expect(
        summary.locator(".plan-stage-row.will-run", has_text="Extract Features")
    ).to_be_visible()
    expect(
        summary.locator(".plan-stage-row.will-run", has_text="Group & Score")
    ).to_be_visible()
    expect(
        summary.locator(".plan-stage-row.done-prior", has_text="Classify")
    ).to_be_visible()


def test_pipeline_plan_summary_updates_on_toggle(live_server, page):
    url = live_server["url"]
    page.goto(f"{url}/pipeline")
    page.click("#card-classify .stage-header")
    page.uncheck("#enableClassify")
    summary = page.locator("[data-testid='pipeline-plan-summary']")
    for label in ("Classify", "Extract Features", "Group & Score"):
        expect(
            summary.locator(".plan-stage-row.will-skip", has_text=label)
        ).to_be_visible()


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


def test_pipeline_failed_status_clears_running_pill(live_server, page):
    """When a stage transitions running -> failed mid-pipeline, the pill must
    flip to 'Failed' and stay there even after refreshPipelineUI() runs.

    Regression test: _stageStateFor checks _runningStages before _stageOutcomes,
    so without explicit failed-status handling the leftover running flag would
    mask the failure and leave the pill stuck on 'Running…'.
    """
    url = live_server["url"]
    page.goto(f"{url}/pipeline")
    # Stage enters running state.
    page.evaluate("""
        _updatePipelineStageUI({stages: {classify: {status: 'running'}}});
    """)
    expect(page.locator("#pillClassify")).to_contain_text("Running")
    # Stage fails mid-run.
    page.evaluate("""
        _updatePipelineStageUI({stages: {classify: {status: 'failed'}}});
    """)
    expect(page.locator("#pillClassify")).to_contain_text("Failed")
    # A subsequent recompute must NOT flip back to Running.
    page.evaluate("refreshPipelineUI();")
    expect(page.locator("#pillClassify")).to_contain_text("Failed")
    expect(page.locator("#pillClassify")).not_to_contain_text("Running")


def test_pipeline_eye_keypoints_pill_will_run_by_default(live_server, page):
    """SuperAnimal weights are auto-downloaded by pipeline_job on first run,
    so the pill defaults to 'Will run' even on a fresh fixture with no
    keypoint models on disk — the user is no longer expected to click a
    Download button before starting the pipeline."""
    url = live_server["url"]
    page.goto(f"{url}/pipeline")
    expect(page.locator("#pillEyeKeypoints")).to_contain_text("Will run")


def test_pipeline_eye_keypoints_toggle_off_marks_will_skip(live_server, page):
    """Unchecking the Eye Keypoints enable checkbox flips its pill to
    'Will skip' without affecting Group, which doesn't depend on it."""
    url = live_server["url"]
    page.goto(f"{url}/pipeline")
    expect(page.locator("#pillEyeKeypoints")).to_contain_text("Will run")
    page.click("#card-eyekeypoints .stage-header")
    page.uncheck("#enableEyeKeypoints")
    expect(page.locator("#pillEyeKeypoints")).to_contain_text("Will skip")
    # Group does not depend on eye keypoints — it must still be runnable.
    expect(page.locator("#pillGroup")).not_to_contain_text("Will skip")


def test_pipeline_disabling_extract_cascades_to_eye_keypoints(live_server, page):
    """Eye Keypoints needs masks from Extract, so toggling Extract off must
    uncheck and disable the Eye Keypoints checkbox alongside Group."""
    url = live_server["url"]
    page.goto(f"{url}/pipeline")
    page.click("#card-extract .stage-header")
    page.uncheck("#enableExtract")
    ek = page.locator("#enableEyeKeypoints")
    expect(ek).not_to_be_checked()
    expect(ek).to_be_disabled()
    expect(page.locator("#pillEyeKeypoints")).to_contain_text("Will skip")


def test_pipeline_previews_pill_shows_pending_count(live_server, page):
    """Previews card must surface honest counts ("Will generate N previews")
    instead of an opaque "Will run", per CORE_PHILOSOPHY's no-black-boxes
    rule. The seeded fixture has 5 photos and no preview_cache rows, so
    the pill should reflect 5 photos pending and the summary should name
    the substages that have work.
    """
    url = live_server["url"]
    page.goto(f"{url}/pipeline")
    expect(page.locator("#pillPreviews")).to_contain_text("Will run")
    # Wait for the plan to actually populate — the pill above has a
    # default-when-no-plan fallback, so its presence isn't proof the
    # plan response has been merged into _pipelinePlan yet.
    page.wait_for_function(
        "() => window._pipelinePlan && window._pipelinePlan.stages "
        "&& window._pipelinePlan.stages.Previews"
    )
    plan = page.evaluate(
        "() => window._pipelinePlan ? window._pipelinePlan.stages.Previews : null"
    )
    assert plan is not None, "Previews plan entry missing from response"
    assert plan["detail"]["eligible"] > 0, f"Expected eligible>0, got: {plan!r}"
    # Detail count appears in parentheses — the pill formatter shape is
    # "Will run (N)" / "Resume (N left)".
    pill_text = page.locator("#pillPreviews").inner_text()
    assert any(c.isdigit() for c in pill_text), (
        f"Previews pill should include a count, got: {pill_text!r}, "
        f"plan: {plan!r}"
    )
    # Summary span carries the substage breakdown ("N previews" or
    # "N thumbnails, N previews").
    expect(page.locator("#summaryPreviews")).to_contain_text("preview")


def test_pipeline_preview_size_is_library_setting(live_server, page):
    """Preview size is a library/workspace policy, not a per-run pipeline
    choice. The pipeline should surface the active value without offering a
    run-local override.
    """
    url = live_server["url"]
    page.goto(f"{url}/pipeline")
    page.click("#card-previews .stage-header")
    expect(page.locator("#cfgPreviewSize")).to_have_count(0)
    expect(page.locator("#cfgPreviewSizeSummary")).to_contain_text("px")


def test_pipeline_shared_card_not_done_until_all_substages_complete(live_server, page):
    """model_loader and classify both map to the Classify card. The pill must
    not flip to 'Done' when only model_loader has completed — classify still
    has work to do. Same shape applies to thumbnails/previews on the Previews
    card."""
    url = live_server["url"]
    page.goto(f"{url}/pipeline")
    page.evaluate("""
        _updatePipelineStageUI({stages: {model_loader: {status: 'running'}}});
    """)
    expect(page.locator("#pillClassify")).to_contain_text("Running")
    # Only model_loader has terminated; classify still has work pending.
    page.evaluate("""
        _updatePipelineStageUI({stages: {model_loader: {status: 'completed'}}});
    """)
    expect(page.locator("#pillClassify")).not_to_contain_text("Done")
    # Both substages terminal → Done.
    page.evaluate("""
        _updatePipelineStageUI({stages: {
            model_loader: {status: 'completed'},
            classify: {status: 'completed'},
        }});
    """)
    expect(page.locator("#pillClassify")).to_contain_text("Done")
