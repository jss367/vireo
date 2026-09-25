import json
import re

import pytest
from playwright.sync_api import expect


def test_pipeline_page_loads_with_stages(live_server, page):
    url = live_server["url"]
    page.goto(f"{url}/pipeline")
    stages = page.locator("[data-testid='stage-card']")
    expect(stages).to_have_count(7)


def test_pipeline_start_button_disabled_without_folders(live_server, page):
    url = live_server["url"]
    page.goto(f"{url}/pipeline")
    btn = page.locator("[data-testid='start-pipeline-btn']")
    expect(btn).to_be_disabled()


@pytest.mark.parametrize("source", ["folders", "collection"])
def test_pipeline_plan_requires_source_and_clears_counts(live_server, page, source):
    """Empty selections must never display or request a workspace-wide plan."""
    collection_id = live_server["db"].add_collection(
        "All test photos", json.dumps({"mode": "all", "rules": []}),
    )
    requests = []
    page.on("request", lambda request: requests.append(request.post_data_json)
            if request.url.endswith("/api/pipeline/plan") else None)
    page.goto(f'{live_server["url"]}/pipeline')
    page.wait_for_function("!window._pageInitPending")
    expect(page.locator("#collectionPicker option", has_text="All test photos")).to_have_count(1)
    if source == "collection":
        page.click("[data-testid='source-collection']")

    summary = page.locator("#pipelinePlanSummary")
    expect(summary).to_have_text("Select a source to see the pipeline plan.")
    expect(page.locator("#pillPreviews")).to_have_text("Select a source")
    expect(page.locator("#pipelineActionStatus")).to_contain_text("Select workspace folders")
    # Let initial model/label loading and the debounce finish before checking
    # that the empty page never requested the API's whole-workspace fallback.
    page.wait_for_timeout(400)
    assert requests == []

    folder = page.locator("#folderScopeList input[type=checkbox]").first
    if source == "folders":
        folder.check()
    else:
        page.select_option("#collectionPicker", str(collection_id))
    expect(page.locator("#summaryPreviews")).to_contain_text("preview")
    expect(page.locator("#btnStartPipeline")).to_be_enabled()
    assert requests
    assert all("folder_ids" in body if source == "folders"
               else body.get("collection_id") == collection_id for body in requests)
    request_count = len(requests)

    if source == "folders":
        folder.uncheck()
    else:
        page.select_option("#collectionPicker", "")
    expect(summary).to_have_text("Select a source to see the pipeline plan.")
    for suffix in ("Scan", "Previews", "Classify", "Extract", "EyeKeypoints", "Group"):
        expect(page.locator(f"#pill{suffix}")).to_have_text("Select a source")
    for selector in ("#summaryPreviews", "#txtDetections", "#txtMasks",
                     "#txtEyeKeypoints", "#txtResults"):
        expect(page.locator(selector)).to_be_empty()
    expect(page.locator(".stage-progress-bar:not(.hidden)")).to_have_count(0)
    expect(page.locator("#btnStartPipeline")).to_be_disabled()
    page.wait_for_timeout(250)
    assert len(requests) == request_count


def test_pipeline_late_plan_cannot_restore_counts_after_source_cleared(live_server, page):
    page.goto(f'{live_server["url"]}/pipeline')
    page.wait_for_function("!window._pageInitPending")
    folder = page.locator("#folderScopeList input[type=checkbox]").first
    folder.check()
    expect(page.locator("#summaryPreviews")).to_contain_text("preview")
    page.wait_for_function("!_planRefreshPending")
    # Hold the next plan response while the user switches to an empty source.
    page.evaluate("""() => {
        const fetch = safeFetch;
        const oldPlan = _pipelinePlan;
        safeFetch = function(url, ...args) {
            if (url === '/api/pipeline/plan') {
                return new Promise(resolve => {
                    window.releasePlan = () => resolve(oldPlan);
                });
            }
            return fetch(url, ...args);
        };
        window.heldPlan = refreshPipelinePlan();
    }""")
    page.click("[data-testid='source-collection']")
    page.evaluate("async () => { releasePlan(); await heldPlan; }")
    expect(page.locator("#pipelinePlanSummary")).to_have_text(
        "Select a source to see the pipeline plan."
    )
    expect(page.locator("#summaryPreviews")).to_be_empty()
    expect(page.locator("#btnStartPipeline")).to_be_disabled()
    assert page.evaluate("_pipelinePlan === null && !_planRefreshPending")


def test_pipeline_has_no_destination_card(live_server, page):
    """The Destination card left with the import/process split — Process
    never copies files, so the page must not offer a destination or any
    of the legacy copy controls."""
    url = live_server["url"]
    page.goto(f"{url}/pipeline")
    expect(page.locator("#card-destination")).to_have_count(0)
    for testid in (
        "file-copying-section",
        "copy-photos-toggle",
        "destination-section",
        "custom-template-input",
        "preview-folders-btn",
        "workspace-new",
        "workspace-current",
    ):
        expect(page.locator(f"[data-testid='{testid}']")).to_have_count(0)


def test_pipeline_source_points_imports_to_import_page(live_server, page):
    url = live_server["url"]
    page.goto(f"{url}/pipeline")
    hint = page.locator("[data-testid='source-import-hint']")
    expect(hint).to_contain_text("Adding new photos to your library happens on the")
    expect(hint.locator("a[href='/import']")).to_contain_text("Import")


def test_pipeline_has_no_source_browse_controls(live_server, page):
    """Arbitrary-path sources left with the import/process split: the
    Source card offers only workspace folders and collections, so the
    Browse button, type-a-path input, folder-browser modal, and new-images
    snapshot source must all be gone."""
    url = live_server["url"]
    page.goto(f"{url}/pipeline")
    expect(page.locator("[data-testid='source-browse-btn']")).to_have_count(0)
    expect(page.locator("#cfgSourceInput")).to_have_count(0)
    expect(page.locator("#folderBrowserOverlay")).to_have_count(0)
    expect(page.locator("#sourceOptionNewImages")).to_have_count(0)
    expect(page.locator("[data-testid='source-new-images']")).to_have_count(0)


def test_pipeline_collection_source_dims_folders(live_server, page):
    url = live_server["url"]
    page.goto(f"{url}/pipeline")
    page.click("[data-testid='source-collection']")
    folders_body = page.locator("#sourceImportBody")
    expect(folders_body).to_have_class(re.compile("dimmed"))


def test_pipeline_folders_source_dims_collection(live_server, page):
    url = live_server["url"]
    page.goto(f"{url}/pipeline")
    page.click("[data-testid='source-collection']")
    collection_body = page.locator("[data-testid='collection-section']")
    expect(collection_body).not_to_have_class(re.compile("dimmed"))
    page.click("[data-testid='source-folders-option']")
    expect(collection_body).to_have_class(re.compile("dimmed"))


def test_pipeline_folder_selection_posts_folder_ids(live_server, page):
    """Switching back from collection mode and checking a workspace folder
    must make Start POST `folder_ids` — not the previously selected
    collection scope."""
    url = live_server["url"]
    page.route(
        re.compile(r"/api/workspaces/\d+/folders$"),
        lambda route: route.fulfill(
            status=200,
            content_type="application/json",
            body=json.dumps([
                {
                    "id": 42,
                    "path": "/library",
                    "parent_id": None,
                    "photo_count": 5,
                    "workspace_photo_count": 5,
                },
            ]),
        ),
    )
    pipeline_payloads = []

    def capture_pipeline(route):
        pipeline_payloads.append(route.request.post_data_json)
        route.fulfill(
            status=200,
            content_type="application/json",
            body=json.dumps({"job_id": "job-p2"}),
        )

    page.route("**/api/jobs/pipeline", capture_pipeline)

    page.goto(f"{url}/pipeline")
    page.click("[data-testid='source-collection']")
    assert page.evaluate("_sourceMode") == "collection"

    # Return to folders scope, then check a workspace folder.
    page.click("[data-testid='source-folders-option']")
    assert page.evaluate("_sourceMode") == "folders"
    folder_cb = page.locator("#folderScopeList input[type='checkbox']").first
    expect(folder_cb).to_be_visible()
    folder_cb.check()

    # Start posts folder_ids, not a collection scope.
    page.uncheck("#enableClassify")
    start_btn = page.locator("[data-testid='start-pipeline-btn']")
    expect(start_btn).to_be_enabled()
    start_btn.click()

    for _ in range(50):
        if pipeline_payloads:
            break
        page.wait_for_timeout(100)
    assert pipeline_payloads, "expected /api/jobs/pipeline to be POSTed"
    body = pipeline_payloads[0]
    assert body.get("folder_ids") == [42], (
        f"expected folder_ids=[42], got body={body!r}"
    )
    assert "collection_id" not in body, (
        f"folders mode must not POST collection_id, got body={body!r}"
    )
    assert "sources" not in body, (
        f"folders mode must not POST sources, got body={body!r}"
    )


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


def test_pipeline_section_headers_visible(live_server, page):
    url = live_server["url"]
    page.goto(f"{url}/pipeline")
    headers = page.locator(".stage-section-header")
    expect(headers).to_have_count(3)
    expect(headers.nth(0)).to_contain_text("Setup")
    expect(headers.nth(1)).to_contain_text("Preparation")
    expect(headers.nth(2)).to_contain_text("AI processing")


def test_pipeline_status_pills_visible_for_processing_stages(live_server, page):
    url = live_server["url"]
    page.goto(f"{url}/pipeline")
    page.locator("#folderScopeList input[type=checkbox]").first.check()
    # Stages 3-8 each have a pill that's populated on load.
    for suffix in ["Scan", "Previews", "Classify", "Extract", "EyeKeypoints", "Group"]:
        pill = page.locator(f"#pill{suffix}")
        expect(pill).to_be_visible()
    # Preparation stages always start at "Will run".
    expect(page.locator("#pillScan")).to_contain_text("Will run")
    # Seeded fixture has detections → Classify shows "Already done".
    expect(page.locator("#pillClassify")).to_contain_text("Already done")
    # Extract has no seeded masks → "Will run".
    expect(page.locator("#pillExtract")).to_contain_text("Will run")


def test_pipeline_reclassify_flips_classify_pill_to_will_run(live_server, page):
    url = live_server["url"]
    page.goto(f"{url}/pipeline")
    page.locator("#folderScopeList input[type=checkbox]").first.check()
    expect(page.locator("#pillClassify")).to_contain_text("Already done")
    page.click("#card-classify .stage-header")
    page.check("#chkReclassify")
    expect(page.locator("#pillClassify")).to_contain_text("Will run")


def test_pipeline_labels_get_more_opens_download_modal(live_server, page):
    url = live_server["url"]
    page.goto(f"{url}/pipeline")
    page.click("#card-classify .stage-header")
    page.get_by_role("button", name="Get more").click()
    modal = page.locator("#pipelineLabelsModal")
    expect(modal).to_have_class(re.compile("open"))
    expect(modal.get_by_role("heading", name="Download Species Labels")).to_be_visible()
    expect(page.locator("#pipelineTaxonCheckboxes")).to_contain_text("Birds")


def test_pipeline_save_process_as_new_uses_in_page_dialog(live_server, page):
    """Desktop webviews can suppress window.prompt(), so process management
    must use a visible in-page dialog and persist through the API."""
    url = live_server["url"]
    db = live_server["db"]
    page.goto(f"{url}/pipeline")

    page.locator("#btnProcessSaveNew").click()
    modal = page.locator("#processEditorModal")
    expect(modal).to_have_class(re.compile(r"\bopen\b"))
    expect(modal.get_by_role("heading", name="Save process as new")).to_be_visible()

    page.locator("#processEditorName").fill("Bird review")
    page.locator("#processEditorSubmitBtn").click()

    expect(modal).not_to_have_class(re.compile(r"\bopen\b"))
    expect(page.locator("#strategySelect")).to_have_value(
        str(next(p["id"] for p in db.get_saved_processes()
                 if p["name"] == "Bird review"))
    )
    expect(page.locator("#processEditorStatus")).to_have_text("Saved “Bird review”.")


def test_pipeline_rename_process_uses_in_page_dialog(live_server, page):
    url = live_server["url"]
    db = live_server["db"]
    process_id = db.create_saved_process("Old process name")
    page.goto(f"{url}/pipeline")
    page.locator("#strategySelect").select_option(str(process_id))

    page.locator("#btnProcessRename").click()
    modal = page.locator("#processEditorModal")
    expect(modal).to_have_class(re.compile(r"\bopen\b"))
    expect(page.locator("#processEditorName")).to_have_value("Old process name")

    page.locator("#processEditorName").fill("Renamed process")
    page.locator("#processEditorSubmitBtn").click()

    expect(modal).not_to_have_class(re.compile(r"\bopen\b"))
    expect(page.locator("#strategySelect option:checked")).to_have_text(
        "Renamed process"
    )
    assert db.get_saved_process(process_id)["name"] == "Renamed process"


def test_pipeline_delete_process_uses_in_page_confirmation(live_server, page):
    url = live_server["url"]
    db = live_server["db"]
    process_id = db.create_saved_process("Disposable process")
    page.goto(f"{url}/pipeline")
    page.locator("#strategySelect").select_option(str(process_id))

    page.locator("#btnProcessDelete").click()
    modal = page.locator("#processEditorModal")
    expect(modal).to_have_class(re.compile(r"\bopen\b"))
    expect(page.locator("#processEditorModalDescription")).to_contain_text(
        "Delete “Disposable process”?"
    )
    page.locator("#processEditorSubmitBtn").click()

    expect(modal).not_to_have_class(re.compile(r"\bopen\b"))
    expect(page.locator("#strategySelect")).to_have_value("__custom__")
    assert db.get_saved_process(process_id) is None


def test_pipeline_delete_dialog_keeps_original_process_target(live_server, page):
    """Changing the page selection behind the open modal must not change
    which process the confirmation deletes."""
    url = live_server["url"]
    db = live_server["db"]
    original_id = db.create_saved_process("Delete this process")
    other_id = db.create_saved_process("Keep this process")
    page.goto(f"{url}/pipeline")
    page.locator("#strategySelect").select_option(str(original_id))

    page.locator("#btnProcessDelete").click()
    expect(page.locator("#processEditorModalDescription")).to_contain_text(
        "Delete “Delete this process”?"
    )

    # Model keyboard/assistive-technology focus escaping the modal and
    # changing the underlying picker while its confirmation remains open.
    page.evaluate(
        """(processId) => {
          const select = document.getElementById('strategySelect');
          select.value = String(processId);
          onProcessSelect();
        }""",
        other_id,
    )
    page.locator("#processEditorSubmitBtn").click()

    expect(page.locator("#processEditorModal")).not_to_have_class(
        re.compile(r"\bopen\b")
    )
    assert db.get_saved_process(original_id) is None
    assert db.get_saved_process(other_id)["name"] == "Keep this process"


def test_pipeline_process_dialog_validates_name_and_closes_with_escape(
    live_server, page
):
    url = live_server["url"]
    page.goto(f"{url}/pipeline")
    save_new = page.locator("#btnProcessSaveNew")
    save_new.click()

    page.locator("#processEditorSubmitBtn").click()
    expect(page.locator("#processEditorError")).to_have_text(
        "Enter a process name."
    )
    expect(page.locator("#processEditorModal")).to_have_class(
        re.compile(r"\bopen\b")
    )

    page.keyboard.press("Escape")
    expect(page.locator("#processEditorModal")).not_to_have_class(
        re.compile(r"\bopen\b")
    )
    expect(save_new).to_be_focused()


def test_pipeline_toggling_classify_off_keeps_group_available(live_server, page):
    url = live_server["url"]
    page.goto(f"{url}/pipeline")
    page.locator("#folderScopeList input[type=checkbox]").first.check()
    page.click("#card-classify .stage-header")
    page.uncheck("#enableClassify")
    for suffix in ["Classify", "Extract"]:
        pill = page.locator(f"#pill{suffix}")
        expect(pill).to_contain_text("Will skip")
    group = page.locator("#enableGroup")
    expect(group).to_be_checked()
    expect(group).to_be_enabled()
    expect(page.locator("#pillGroup")).not_to_contain_text("Will skip")
    # Preparation stages are unaffected.
    expect(page.locator("#pillScan")).to_contain_text("Will run")


def test_pipeline_plan_summary_lists_stages(live_server, page):
    url = live_server["url"]
    page.goto(f"{url}/pipeline")
    page.locator("#folderScopeList input[type=checkbox]").first.check()
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
    page.locator("#folderScopeList input[type=checkbox]").first.check()
    page.click("#card-classify .stage-header")
    page.uncheck("#enableClassify")
    summary = page.locator("[data-testid='pipeline-plan-summary']")
    for label in ("Classify", "Extract Features"):
        expect(
            summary.locator(".plan-stage-row.will-skip", has_text=label)
        ).to_be_visible()
    expect(
        summary.locator(".plan-stage-row.will-run", has_text="Group & Score")
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
    page.locator("#folderScopeList input[type=checkbox]").first.check()
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
    page.locator("#folderScopeList input[type=checkbox]").first.check()
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


def test_pipeline_eye_keypoints_pill_will_skip_by_default(live_server, page):
    """Eye-keypoint detection is opt-in, so a fresh fixture skips it.

    Model readiness does not control this state: SuperAnimal weights are
    downloaded automatically if the user explicitly enables the stage.
    """
    url = live_server["url"]
    page.goto(f"{url}/pipeline")
    page.locator("#folderScopeList input[type=checkbox]").first.check()
    # Wait for /api/pipeline/page-init to settle so the checkbox reflects
    # cfg.eye_detect_enabled (not the HTML default), then wait for the
    # initial /api/pipeline/plan. Without the page-init wait the checkbox
    # could still flip during the assertions; without the plan wait the
    # pill could read "Will skip" from the fallback (checkbox unchecked)
    # even if the server-side plan has flipped Eye Keypoints back to
    # opt-in-by-default — the very regression this test guards.
    page.wait_for_function("() => window._pageInitPending === false")
    page.wait_for_function(
        "() => window._pipelinePlan && window._pipelinePlan.stages "
        "&& window._pipelinePlan.stages.EyeKeypoints "
        "&& window._pipelinePlan.stages.EyeKeypoints.state === 'will-skip' "
        "&& !window._planRefreshPending"
    )
    expect(page.locator("#enableEyeKeypoints")).not_to_be_checked()
    expect(page.locator("#pillEyeKeypoints")).to_contain_text("Will skip")


def test_pipeline_eye_keypoints_toggle_off_marks_will_skip(live_server, page):
    """Explicitly enabling then disabling Eye Keypoints updates its pill.

    Group remains runnable because it does not depend on eye keypoints.
    """
    url = live_server["url"]
    page.goto(f"{url}/pipeline")
    page.locator("#folderScopeList input[type=checkbox]").first.check()
    # Wait for /api/pipeline/page-init to settle first. Its success handler
    # assigns enableEyeKeypoints.checked from cfg.eye_detect_enabled; if it
    # ran after page.check() below, it would silently overwrite the opt-in
    # and the later "will-run" wait would time out.
    page.wait_for_function("() => window._pageInitPending === false")
    page.click("#card-eyekeypoints .stage-header")
    page.check("#enableEyeKeypoints")
    # Wait for the debounced plan refresh to confirm 'will-run'. Reading
    # only the pill would pass from the null-plan fallback in
    # _stageStateFor, so a broken eye_detect_override wiring (plan comes
    # back "will-skip" after opt-in) would be masked here.
    page.wait_for_function(
        "() => window._pipelinePlan && window._pipelinePlan.stages "
        "&& window._pipelinePlan.stages.EyeKeypoints "
        "&& window._pipelinePlan.stages.EyeKeypoints.state === 'will-run' "
        "&& !window._planRefreshPending"
    )
    expect(page.locator("#pillEyeKeypoints")).to_contain_text("Will run")
    page.uncheck("#enableEyeKeypoints")
    page.wait_for_function(
        "() => window._pipelinePlan && window._pipelinePlan.stages "
        "&& window._pipelinePlan.stages.EyeKeypoints "
        "&& window._pipelinePlan.stages.EyeKeypoints.state === 'will-skip' "
        "&& !window._planRefreshPending"
    )
    expect(page.locator("#pillEyeKeypoints")).to_contain_text("Will skip")
    # Group does not depend on eye keypoints — it must still be runnable.
    expect(page.locator("#pillGroup")).not_to_contain_text("Will skip")


def test_pipeline_disabling_extract_keeps_group_available(live_server, page):
    """Eye Keypoints needs masks from Extract, so toggling Extract off must
    disable Eye Keypoints while leaving cached-feature grouping available."""
    url = live_server["url"]
    page.goto(f"{url}/pipeline")
    page.locator("#folderScopeList input[type=checkbox]").first.check()
    page.click("#card-extract .stage-header")
    page.uncheck("#enableExtract")
    ek = page.locator("#enableEyeKeypoints")
    expect(ek).not_to_be_checked()
    expect(ek).to_be_disabled()
    expect(page.locator("#pillEyeKeypoints")).to_contain_text("Will skip")
    group = page.locator("#enableGroup")
    expect(group).to_be_checked()
    expect(group).to_be_enabled()
    expect(page.locator("#pillGroup")).not_to_contain_text("Will skip")


def test_pipeline_previews_pill_shows_pending_count(live_server, page):
    """Previews card must surface honest counts ("Will generate N previews")
    instead of an opaque "Will run", per CORE_PHILOSOPHY's no-black-boxes
    rule. The selected folder has 3 photos and no preview_cache rows, so
    the pill should reflect that scope and name the substages with work.
    """
    url = live_server["url"]
    page.goto(f"{url}/pipeline")
    page.locator("#folderScopeList input[type=checkbox]").first.check()
    expect(page.locator("#pillPreviews")).to_contain_text("Will run")
    # Verify the counts are scoped to the selected folder.
    page.wait_for_function(
        "() => window._pipelinePlan && window._pipelinePlan.stages "
        "&& window._pipelinePlan.stages.Previews"
    )
    plan = page.evaluate(
        "() => window._pipelinePlan ? window._pipelinePlan.stages.Previews : null"
    )
    assert plan is not None, "Previews plan entry missing from response"
    assert plan["detail"]["eligible"] == 3, f"Expected selected folder's 3 photos, got: {plan!r}"
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


def test_pipeline_extract_card_reports_unreadable_photos(live_server, page):
    """The Extract card must show what actually happened to the photos.

    Two failures met here in production: the card read `masks_created` /
    `scored_count`, field names the backend never emits, so its summary was
    always blank; and unreadable sources were counted as ordinary skips. A
    dropped share therefore rendered as an empty, green "Done!" card while
    311 of 706 photos went unmasked — and every unmasked photo is then
    hard-rejected in Process Review as `no_subject_mask`.
    """
    url = live_server["url"]
    page.goto(f"{url}/pipeline")
    page.evaluate("""
        _onPipelineComplete({
          status: 'failed',
          result: {
            stages: {extract_masks: {
              masked: 395, skipped: 0, unreadable: 311, failed: 0, total: 706,
            }},
            errors: [
              '[extract_masks] Fatal: 311 of 706 photos unreachable — ' +
              'volume /Volumes/Photography is not mounted.',
            ],
          },
        });
    """)
    summary = page.locator("#txtMasks")
    expect(summary).to_contain_text("395 masked")
    expect(summary).to_contain_text("311 unreadable")
    expect(page.locator("#pillExtract")).to_contain_text("Failed")
    expect(page.locator("#statusExtract")).to_contain_text("unreachable")


def test_pipeline_extract_card_does_not_claim_no_masks_were_needed(
    live_server, page,
):
    """An empty worklist the backend emptied because something went wrong is
    not the same as "nothing needed doing".

    When mask extraction reports a `reason` (no detections, missing weights,
    everything below the detector threshold, source offline at preflight),
    every counter comes back zero. Rendering that as "No photos needed masks"
    contradicts the error the same card is showing and tells the user their
    library is fine when it is about to be hard-rejected as `no_subject_mask`.
    """
    url = live_server["url"]
    page.goto(f"{url}/pipeline")
    page.evaluate("""
        _onPipelineComplete({
          status: 'failed',
          result: {
            stages: {extract_masks: {
              masked: 0, skipped: 0, failed: 0, unreadable: 0, total: 0,
              reason: 'weights_missing',
            }},
            errors: [
              '[extract_masks] No detections available for 984 photo(s). ' +
              'MegaDetector weights are not downloaded.',
            ],
          },
        });
    """)
    expect(page.locator("#txtMasks")).not_to_contain_text(
        "No photos needed masks"
    )


def test_pipeline_extract_card_not_green_for_zero_mask_reasons(
    live_server, page,
):
    """A `reason` outcome makes no masks, so the card must not keep the
    green completed styling while its own pill reads Failed.

    `weights_missing` / `no_detections` / `all_subthreshold` all leave the
    per-photo counters at zero, so a clean check that only looks at
    `unreadable` and `failed` reads them as a flawless run.
    """
    url = live_server["url"]
    page.goto(f"{url}/pipeline")
    page.evaluate("""
        _onPipelineComplete({
          status: 'failed',
          result: {
            stages: {extract_masks: {
              masked: 0, skipped: 0, failed: 0, unreadable: 0, total: 0,
              reason: 'weights_missing',
            }},
            errors: [
              '[extract_masks] No detections available for 984 photo(s). ' +
              'MegaDetector weights are not downloaded.',
            ],
          },
        });
    """)
    expect(page.locator("#numExtract")).not_to_have_class(re.compile("complete"))
    expect(page.locator("#statusExtract")).not_to_contain_text("Done!")


def test_pipeline_card_shows_actionable_error_not_the_last_one(
    live_server, page,
):
    """When a stage reports several errors, the card must keep the actionable
    `Fatal:` one rather than whichever happened to be appended last.

    The errors loop keyed by stage with last-value-wins, so a generic
    "failed mask extraction" line appended after a source-outage Fatal buried
    the reconnect instruction — the only message that tells the user what to
    actually do (Codex #1392 P2).
    """
    url = live_server["url"]
    page.goto(f"{url}/pipeline")
    page.evaluate("""
        _onPipelineComplete({
          status: 'failed',
          result: {
            stages: {extract_masks: {
              masked: 0, skipped: 0, failed: 1, unreadable: 1, total: 2,
            }},
            errors: [
              '[extract_masks] Fatal: 1 of 2 photos unreachable — volume ' +
              '/Volumes/Photography is not mounted. Reconnect the source ' +
              'and run Process again.',
              '[extract_masks] 1 of 2 photos failed mask extraction',
            ],
          },
        });
    """)
    expect(page.locator("#statusExtract")).to_contain_text("Reconnect the source")


def test_pipeline_collection_failure_fails_scan_card_on_terminal_path(
    live_server, page,
):
    """A collection failure reaching only the terminal completion payload must
    still fail the Scan card.

    ``collection`` is a failure-only backend stage that shares the Scan card.
    ``_updatePipelineStageUI`` already maps it through ``_failureOnlyStageToCard``,
    but the SSE stream does not replay buffered progress events, so a fast
    collection failure (locked DB at snapshot resolution, empty scope, etc.)
    can arrive before the browser subscribes. ``_onPipelineComplete`` is then
    the only path that renders it; keying its errors loop through
    ``_stageToCard`` alone drops the entry, leaves the successfully-finished
    scan marked Done, and hides the reason the run stopped (Codex #1816 P2).
    """
    url = live_server["url"]
    page.goto(f"{url}/pipeline")
    page.evaluate("""
        _onPipelineComplete({
          status: 'failed',
          result: {
            stages: {
              scan: {status: 'completed', photos_indexed: 42},
              collection: {status: 'failed'},
            },
            errors: [
              '[collection] Fatal: could not resolve snapshot path — ' +
              'database is locked.',
            ],
          },
        });
    """)
    expect(page.locator("#pillScan")).to_contain_text("Failed")
    expect(page.locator("#statusScan")).to_contain_text("could not resolve")
    expect(page.locator("#numScan")).not_to_have_class(re.compile("complete"))


def test_extract_masks_clean_rejects_unreadable_and_failed_results(
    live_server, page,
):
    """The shared clean check both Extract completion paths use.

    `runExtractStep` (the standalone card) marked itself green whenever the
    job status was `completed`, ignoring the counters entirely — so a dropped
    share rendered as "Done!" there even after the pipeline path learned not
    to (Codex #1392 P1).
    """
    url = live_server["url"]
    page.goto(f"{url}/pipeline")
    clean = lambda ext: page.evaluate(  # noqa: E731
        "ext => _extractMasksClean(ext)", ext
    )
    assert clean({"masked": 3, "skipped": 1, "unreadable": 0, "failed": 0,
                  "total": 4}) is True
    assert clean({"masked": 3, "skipped": 0, "unreadable": 1, "failed": 0,
                  "total": 4}) is False
    assert clean({"masked": 3, "skipped": 0, "unreadable": 0, "failed": 1,
                  "total": 4}) is False
    assert clean({"masked": 0, "skipped": 0, "unreadable": 0, "failed": 0,
                  "total": 0, "reason": "weights_missing"}) is False


def test_extract_step_outcome_surfaces_backend_error_on_failed_job(
    live_server, page,
):
    """A standalone Extract run that fails must still show why.

    Making the endpoint return `ok: false` for unreadable photos flipped the
    job status to `failed`, which sent the card down a generic "Failed"
    branch that rendered neither the counts nor the reconnect instruction the
    backend had just put in `result.errors` (Codex #1392 P2).
    """
    url = live_server["url"]
    page.goto(f"{url}/pipeline")
    outcome = page.evaluate(
        "a => _extractStepOutcome(a[0], a[1])",
        ["failed", {
            "masked": 3, "skipped": 0, "unreadable": 2, "failed": 0,
            "total": 5,
            "errors": [
                "2 of 5 photos could not be read, so they have no mask. "
                "Reconnect the source and run Extract again."
            ],
        }],
    )
    assert outcome["clean"] is False
    assert "3 masked" in outcome["summary"]
    assert "2 unreadable" in outcome["summary"], (
        f"The counts must survive a failed job; got {outcome!r}"
    )
    assert "Reconnect the source" in outcome["status"], (
        f"The actionable backend error must reach the card; got {outcome!r}"
    )


def test_extract_step_outcome_clean_run_still_reads_done(live_server, page):
    url = live_server["url"]
    page.goto(f"{url}/pipeline")
    outcome = page.evaluate(
        "a => _extractStepOutcome(a[0], a[1])",
        ["completed", {"masked": 5, "skipped": 0, "unreadable": 0,
                       "failed": 0, "total": 5}],
    )
    assert outcome["clean"] is True
    assert outcome["status"] == "Done!"
    assert "5 masked" in outcome["summary"]


def test_extract_step_outcome_surfaces_top_level_error_on_raised_worker(
    live_server, page,
):
    """A worker that raises leaves `result: null` and stashes the exception
    in the completion payload's top-level `errors`. Reading only nested
    `result.errors` hid the real cause and rendered the generic
    "Some photos have no mask" instead (Codex #1392 P2).
    """
    url = live_server["url"]
    page.goto(f"{url}/pipeline")
    outcome = page.evaluate(
        "a => _extractStepOutcome(a[0], a[1], a[2])",
        ["failed", None, ["SAM2 checkpoint corrupt: unexpected EOF"]],
    )
    assert outcome["clean"] is False
    assert "SAM2 checkpoint corrupt" in outcome["status"], (
        f"The top-level exception must reach the card when the worker "
        f"raised; got {outcome!r}"
    )


def test_extract_step_outcome_labels_bare_cancellation(live_server, page):
    """A cancel arriving before the worker returned a structured result must
    read "Cancelled", not the generic "Some photos have no mask" fallback
    (Codex #1392 P2).
    """
    url = live_server["url"]
    page.goto(f"{url}/pipeline")
    outcome = page.evaluate(
        "a => _extractStepOutcome(a[0], a[1], a[2])",
        ["cancelled", None, []],
    )
    assert outcome["clean"] is False
    assert outcome["status"] == "Cancelled", (
        f"A bare cancel must say so, not lie about missing masks; "
        f"got {outcome!r}"
    )


def test_extract_step_outcome_prefers_nested_over_top_level_errors(
    live_server, page,
):
    """When both nested and top-level errors exist, the nested (worker's own)
    error is the actionable one — top-level is only the fallback for the
    raised/cancelled paths.
    """
    url = live_server["url"]
    page.goto(f"{url}/pipeline")
    outcome = page.evaluate(
        "a => _extractStepOutcome(a[0], a[1], a[2])",
        ["failed", {
            "masked": 1, "skipped": 0, "unreadable": 2, "failed": 0,
            "total": 3,
            "errors": ["Reconnect the source and run Extract again."],
        }, ["duplicate of the nested error"]],
    )
    assert "Reconnect" in outcome["status"], (
        f"Nested error wins over top-level; got {outcome!r}"
    )
