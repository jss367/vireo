import re

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
