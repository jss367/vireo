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
