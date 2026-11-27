"""Reviewing a report, changing consent, and downloading the approved payload."""

import json

from playwright.sync_api import expect


def test_review_then_download_description_alone(live_server, page):
    import config as cfg

    cfg.save({**cfg.load(), "report_url": ""})
    page.goto(live_server["url"] + "/browse")
    page.locator("#reportToggle").click()
    page.get_by_label("Issue description").fill("A thumbnail is missing")
    diagnostics = page.get_by_label("Include diagnostics")
    expect(diagnostics).not_to_be_checked()
    diagnostics.check()
    page.get_by_role("button", name="Review report").click()
    preview = page.locator("#reportJson")
    expect(preview).to_be_visible()
    assert "logs" in json.loads(preview.inner_text())

    diagnostics.uncheck()
    expect(preview).to_be_hidden()
    page.get_by_role("button", name="Review report").click()
    download_button = page.get_by_role("button", name="Download report")
    expect(download_button).to_be_visible()
    reviewed = json.loads(preview.inner_text())
    assert reviewed == {"description": "A thumbnail is missing"}
    with page.expect_download() as download:
        download_button.click()
    assert json.loads(download.value.path().read_text()) == reviewed
    expect(page.get_by_role("link", name="Vireo’s public issue tracker")).to_be_visible()
