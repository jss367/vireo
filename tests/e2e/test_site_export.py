"""Site export dialog submits scope/options and keeps failures actionable."""

import json

from playwright.sync_api import expect


def test_site_export_dialog_starts_complete_export(live_server, page, tmp_path):
    requests = []

    def start(route):
        requests.append(json.loads(route.request.post_data))
        route.fulfill(status=200, content_type='application/json', body='{"job_id":"site-export-test"}')

    page.route('**/api/jobs/export-site', start)
    page.goto(f"{live_server['url']}/life-list")
    page.get_by_role('button', name='Export Entire Site', exact=True).click()
    modal = page.locator('#siteExportModal')
    expect(modal).to_be_visible()
    expect(modal).to_contain_text('Photos in multiple albums are saved once')
    expect(page.locator('#siteExportLocations')).not_to_be_checked()
    page.locator('#siteExportSubmit').click()
    expect(page.locator('#siteExportStatus')).to_have_text('Destination folder is required.')
    destination = str(tmp_path / 'export')
    page.locator('#siteExportDest').fill(destination)
    page.locator('#siteExportLocations').check()
    page.locator('#siteExportSubmit').click()
    expect(modal).not_to_be_visible()
    assert requests == [{'destination': destination, 'include_locations': True}]


def test_site_export_start_failure_can_be_retried(live_server, page, tmp_path):
    page.route('**/api/jobs/export-site', lambda route: route.fulfill(
        status=400, content_type='application/json', body='{"error":"Destination unavailable"}',
    ))
    page.goto(f"{live_server['url']}/life-list")
    page.get_by_role('button', name='Export Entire Site', exact=True).click()
    page.locator('#siteExportDest').fill(str(tmp_path / 'export'))
    page.locator('#siteExportSubmit').click()
    expect(page.locator('#siteExportStatus')).to_contain_text('Could not start site export')
    expect(page.locator('#siteExportModal')).to_be_visible()
    expect(page.locator('#siteExportSubmit')).to_be_enabled()
    expect(page.locator('#siteExportDest')).to_be_enabled()
    page.locator('#siteExportModal').get_by_role('button', name='Cancel').click()
    expect(page.locator('#siteExportModal')).not_to_be_visible()
