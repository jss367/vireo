"""Custom export choices survive dialog resets and navigation between pages."""

import pytest
from playwright.sync_api import expect


def open_export(page, live_server, view):
    if view == "browse":
        page.goto(f"{live_server['url']}/browse")
        page.locator(".grid-card").first.click()
        page.get_by_role("button", name="Export", exact=True).click()
    else:
        photo_id = live_server["data"]["photos"][0]
        page.goto(f"{live_server['url']}/edit/{photo_id}")
        page.locator("#exportBtn").click()
    expect(page.locator("#exportSubmitBtn")).to_be_enabled()


@pytest.mark.parametrize("source", ["browse", "editor"])
def test_custom_export_settings_survive_navigation(live_server, page, source, tmp_path):
    open_export(page, live_server, source)
    destination = str(tmp_path)
    page.locator("#exportDest").fill(destination)
    page.locator("#exportSubfolder").check()
    page.locator("#exportSubfolderName").fill("favorites")
    page.locator("#exportRevealAfter").check()
    page.locator("#exportTemplate").fill("{original}_share")
    page.locator("#exportResize").select_option("custom")
    page.locator("#exportResizeCustom").fill("2345")
    page.locator("#exportQuality").fill("81")
    page.locator("#exportFormat").select_option("tiff")
    page.locator("#exportMetadataLocation").check()
    page.locator("#exportMetadataSpecies").check()
    if source == "editor":
        page.locator("#exportMetadataCaptureDate").check()
    else:
        page.locator("#exportMetadataCaptureDateTime").check()
    page.locator("#exportOverlay [data-export-cancel]").click()

    for view in [source, "editor" if source == "browse" else "browse", source]:
        open_export(page, live_server, view)
        expect(page.locator("#exportPreset")).to_have_value("custom")
        expect(page.locator("#exportDest")).to_have_value(destination)
        expect(page.locator("#exportSubfolder")).to_be_checked()
        expect(page.locator("#exportSubfolderName")).to_have_value("favorites")
        expect(page.locator("#exportRevealAfter")).to_be_checked()
        expect(page.locator("#exportTemplate")).to_have_value("{original}_share")
        expect(page.locator("#exportFormat")).to_have_value("tiff")
        expect(page.locator("#exportResize")).to_have_value("custom")
        expect(page.locator("#exportResizeCustom")).to_have_value("2345")
        expect(page.locator("#exportQuality")).to_have_value("81")
        expect(page.locator("#exportMetadataLocation")).to_be_checked()
        expect(page.locator("#exportMetadataSpecies")).to_be_checked()
        if view == "editor":
            expect(page.locator("#exportMetadataCaptureDate")).to_be_checked()
            if source == "editor":
                expect(page.locator("#exportMetadataCaptureTime")).not_to_be_checked()
            else:
                expect(page.locator("#exportMetadataCaptureTime")).to_be_checked()
        page.locator("#exportTemplate").fill("{original}_share")
        page.locator("#exportOverlay [data-export-cancel]").click()


@pytest.mark.parametrize("stored", [None, "broken json", "null", "[]"])
def test_custom_export_without_valid_snapshot_still_opens(live_server, page, stored):
    page.goto(f"{live_server['url']}/browse")
    page.evaluate("""stored => {
        localStorage.setItem('vireo.export.lastPreset', 'custom');
        if (stored !== null) localStorage.setItem('vireo.export.customSettings', stored);
    }""", stored)
    open_export(page, live_server, "browse")
    expect(page.locator("#exportPreset")).to_have_value("custom")
    expect(page.locator("#exportFormat")).to_have_value("jpg")
