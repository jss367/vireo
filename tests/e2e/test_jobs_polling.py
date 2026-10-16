"""Jobs refresh failures stay visible without replacing known-good progress."""

import pytest
from playwright.sync_api import expect

JOB = {
    "id": "polling-test", "type": "scan", "status": "running",
    "started_at": "2026-10-16T00:00:00", "config": {}, "steps": [], "errors": [],
    "progress": {"current": 7, "total": 10, "phase": "Polling test progress"},
}


def mount_jobs(live_server, page, initial_failure=None):
    page.clock.install()
    page.clock.pause_at(page.evaluate("Date.now() / 1000 + 1"))
    active = {"active": [] if initial_failure else [JOB]}
    history = [] if initial_failure else [{
        **JOB, "id": "history-test", "status": "completed", "summary": "Saved history row",
    }]
    page.route("**/api/jobs", lambda route: route.fulfill(
        status=503 if initial_failure == "active" else 200,
        json={"error": "Unavailable"} if initial_failure == "active" else active,
    ))
    page.route("**/api/jobs/history?*", lambda route: route.fulfill(
        status=503 if initial_failure == "history" else 200,
        json={"error": "Unavailable"} if initial_failure == "history" else history,
    ))
    page.goto(live_server["url"] + "/jobs")
    page.wait_for_function("""Object.values(jobsPollState).every(lane =>
      !lane.pending && (lane.loaded || lane.error))""")
    page.evaluate("""() => {
      window.pollRequests = [];
      const originalFetch = window.fetch;
      window.fetch = (url, options) => String(url).startsWith('/api/jobs')
        ? new Promise((resolve, reject) => pollRequests.push({url, options, resolve, reject}))
        : originalFetch(url, options);
    }""")


@pytest.mark.parametrize("lane", ["active", "history"])
@pytest.mark.parametrize("failure", ["network", "http", "json", "shape"])
def test_jobs_refresh_failure_keeps_data_and_keyboard_retry_recovers(live_server, page, lane, failure):
    mount_jobs(live_server, page)
    page.evaluate("""({lane, failure}) => {
      window.failedRefresh = lane === 'active' ? fetchJobs() : fetchHistory();
      const request = pollRequests[0];
      if (failure === 'network') request.reject(new Error('offline'));
      else if (failure === 'json') request.resolve({ok: true, json: async () => { throw new Error('invalid JSON'); }});
      else request.resolve({ok: failure !== 'http', json: async () => ({error: '<script>bad</script>'})});
    }""", {"lane": lane, "failure": failure})
    page.evaluate("async () => { await failedRefresh; }")
    warning = page.locator("#jobsPollWarning")
    expect(warning).to_be_visible()
    expect(warning).to_contain_text("Showing the last successful list")
    expect(warning.locator("script")).to_have_count(0)
    expect(page.locator('#jobListContent [data-job-id="polling-test"]')).to_be_visible()
    expect(page.locator('#jobListContent [data-job-id="history-test"]')).to_be_visible()
    retry = warning.get_by_role("button", name="Retry now", exact=True)
    retry.focus()
    retry.press("Enter")
    expect(warning.get_by_role("button", name="Retrying…", exact=True)).to_be_disabled()
    assert page.evaluate("pollRequests.length") == 2
    page.evaluate("""async lane => {
      pollRequests[1].resolve({ok: true, json: async () => lane === 'active' ? {active: []} : []});
    }""", lane)
    expect(warning).to_be_hidden()
    removed = "polling-test" if lane == "active" else "history-test"
    kept = "history-test" if lane == "active" else "polling-test"
    expect(page.locator(f'#jobListContent [data-job-id="{removed}"]')).to_have_count(0)
    expect(page.locator(f'#jobListContent [data-job-id="{kept}"]')).to_be_visible()


def test_jobs_poll_timer_does_not_overlap_and_automatically_recovers(live_server, page):
    mount_jobs(live_server, page)
    page.evaluate("() => { window.slowRefresh = fetchJobs(); }")
    page.clock.fast_forward(4000)
    # The navbar has a separate poller; count only requests belonging to the
    # Jobs page's controller, whose fetch always carries an abort signal.
    assert page.evaluate("pollRequests.filter(r => r.options?.signal).length") == 1
    page.evaluate("""async () => {
      pollRequests.find(r => r.options?.signal).reject(new Error('offline'));
      await slowRefresh;
    }""")
    expect(page.locator("#jobsPollWarning")).to_be_visible()
    page.clock.fast_forward(2000)
    page.wait_for_function("pollRequests.filter(r => r.options?.signal).length === 2")
    page.evaluate("""() => {
      pollRequests.filter(r => r.options?.signal)[1].resolve({ok: true, json: async () => ({active: []})});
    }""")
    expect(page.locator("#jobsPollWarning")).to_be_hidden()


@pytest.mark.parametrize("lane", ["active", "history"])
def test_failed_initial_load_is_not_reported_as_an_empty_list(live_server, page, lane):
    mount_jobs(live_server, page, initial_failure=lane)
    warning = page.locator("#jobsPollWarning")
    expect(warning).to_be_visible()
    expect(warning).to_contain_text("could not be loaded")
    expect(warning).not_to_contain_text("Showing the last successful list")
    expect(page.locator("#jobListContent")).not_to_contain_text("No jobs yet")
    if lane == "history":
        page.locator('.jobs-list-divider[data-view="history"]').click()
    expect(page.locator("#jobDetailPane")).to_contain_text("could not be loaded")
    warning.get_by_role("button", name="Retry now", exact=True).click()
    page.evaluate("""lane => {
      pollRequests[0].resolve({ok: true, json: async () => lane === 'active' ? {active: []} : []});
    }""", lane)
    expect(warning).to_be_hidden()
    expect(page.locator("#jobListContent")).to_contain_text("No jobs yet")
    expect(page.locator("#jobDetailPane")).to_contain_text("No active jobs" if lane == "active" else "No job history")
