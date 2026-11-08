"""Custom export choices survive dialog resets and navigation between pages."""

import json

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


@pytest.mark.parametrize("source", ["browse", "editor"])
@pytest.mark.parametrize("stored", [
    None, "broken json", "{}", '{"format":"png"}',
    json.dumps({
        "destination": "", "export_to_subfolder": "false", "subfolder_name": "exported",
        "reveal_after_export": False, "format": "jpg", "max_size": None,
        "quality": 92, "naming_template": "{original}", "metadata_fields": [],
    }),
    *[json.dumps({
        "destination": "", "export_to_subfolder": False, "subfolder_name": "exported",
        "reveal_after_export": False, "format": "jpg", "max_size": None,
        "quality": 92, "naming_template": "{original}", "metadata_fields": fields,
    }) for fields in ([42], ["invented"], ["location", "invented"])],
])
def test_legacy_custom_preferences_migrate_across_pages(live_server, page, source, stored):
    page.goto(f"{live_server['url']}/browse")
    page.evaluate("""stored => {
        localStorage.setItem('vireo.export.lastPreset', 'custom');
        if (stored !== null) localStorage.setItem('vireo.export.customSettings', stored);
        for (const [key, value] of Object.entries({
            subfolder: '1', subfolderName: 'keepers', revealAfter: '1',
            'metadata.location': '1', 'metadata.captureDateTime': '1',
            'metadata.captureDateTime.presetFields': 'capture_date',
        })) localStorage.setItem('vireo.browse.export.' + key, value);
    }""", stored)
    for view in [source, "editor" if source == "browse" else "browse"]:
        open_export(page, live_server, view)
        expect(page.locator("#exportMetadataLocation")).to_be_checked()
        expect(page.locator("#exportSubfolder")).to_be_checked()
        expect(page.locator("#exportSubfolderName")).to_have_value("keepers")
        expect(page.locator("#exportRevealAfter")).to_be_checked()
        if view == "editor":
            expect(page.locator("#exportMetadataCaptureDate")).to_be_checked()
            expect(page.locator("#exportMetadataCaptureTime")).not_to_be_checked()
        snapshot = page.evaluate("JSON.parse(localStorage.getItem('vireo.export.customSettings'))")
        assert snapshot["metadata_fields"] == ["capture_date", "location"]
        page.locator("#exportOverlay [data-export-cancel]").click()


@pytest.mark.parametrize("stale_snapshot,previous_preset", [
    (False, "web-jpg"), (True, "web-jpg"), (True, "custom"),
])
def test_snapshot_write_failure_does_not_change_last_preset(
    live_server, page, stale_snapshot, previous_preset,
):
    open_export(page, live_server, "browse")
    if stale_snapshot:
        page.locator("#exportTemplate").fill("old_{original}")
    page.locator("#exportPreset").select_option(previous_preset)
    previous = page.evaluate("localStorage.getItem('vireo.export.customSettings')")
    page.evaluate("""() => {
        const original = Storage.prototype.setItem;
        window.restoreExportStorage = () => { Storage.prototype.setItem = original; };
        Storage.prototype.setItem = function(key, value) {
            if (key === 'vireo.export.customSettings') {
                throw new DOMException('Storage quota exceeded', 'QuotaExceededError');
            }
            return original.call(this, key, value);
        };
    }""")
    page.locator("#exportTemplate").fill("new_{original}")
    expect(page.locator("#exportPreset")).to_have_value("custom")
    expect(page.locator("#exportTemplate")).to_have_value("new_{original}")
    assert page.evaluate("localStorage.getItem('vireo.export.lastPreset')") == previous_preset
    assert page.evaluate("localStorage.getItem('vireo.export.customSettings')") == previous
    expect(page.get_by_text("Could not remember export settings. You can still export with these settings.", exact=True)).to_be_visible()
    expect(page.locator("#exportSubmitBtn")).to_be_enabled()
    page.evaluate("window.restoreExportStorage()")
    if previous_preset == "custom":
        # A failed save retains the last durable settings, rather than
        # destroying them and silently substituting factory defaults.
        open_export(page, live_server, "editor")
        expect(page.locator("#exportPreset")).to_have_value("custom")
        expect(page.locator("#exportTemplate")).to_have_value("old_{original}")
    page.locator("#exportTemplate").fill("recovered_{original}")
    assert page.evaluate("localStorage.getItem('vireo.export.lastPreset')") == "custom"
    open_export(page, live_server, "editor")
    expect(page.locator("#exportTemplate")).to_have_value("recovered_{original}")


@pytest.mark.parametrize("legacy_date,legacy_time,combined,expected", [
    ("1", "0", None, True),
    ("0", "1", None, True),
    ("0", "0", None, False),
    ("1", "1", "0", False),
])
def test_editor_migrates_old_capture_keys_before_snapshot(
    live_server, page, legacy_date, legacy_time, combined, expected,
):
    page.goto(f"{live_server['url']}/browse")
    page.evaluate("""([date, time, combined]) => {
        localStorage.setItem('vireo.export.lastPreset', 'custom');
        localStorage.setItem('vireo.browse.export.metadata.captureDate', date);
        localStorage.setItem('vireo.browse.export.metadata.captureTime', time);
        if (combined !== null) {
            localStorage.setItem('vireo.browse.export.metadata.captureDateTime', combined);
        }
    }""", [legacy_date, legacy_time, combined])
    open_export(page, live_server, "editor")
    expect(page.locator("#exportMetadataCaptureDate")).to_be_checked(checked=expected)
    expect(page.locator("#exportMetadataCaptureTime")).to_be_checked(checked=expected)
    snapshot = page.evaluate("JSON.parse(localStorage.getItem('vireo.export.customSettings'))")
    assert snapshot["metadata_fields"] == (["capture_date", "capture_time"] if expected else [])
    open_export(page, live_server, "browse")
    expect(page.locator("#exportMetadataCaptureDateTime")).to_be_checked(checked=expected)
