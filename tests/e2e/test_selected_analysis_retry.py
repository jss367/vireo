"""Selected analysis stays bounded, observable, and separate from decisions."""
from playwright.sync_api import expect

from e2e.test_pipeline_review_species import _write_predictionless_pipeline_cache


def _open_review(live_server, page):
    ids = live_server["data"]["photos"][:3]
    _write_predictionless_pipeline_cache(live_server, ids)
    page.goto(f"{live_server['url']}/pipeline/review")
    page.wait_for_function("pipelineResults && pipelineResults.photos.length === 3")
    page.evaluate("pid => openGroupReview(0, 0, pid)", ids[0])
    page.wait_for_function("grmState && grmState.seeded")
    return ids


def _install_retry(live_server, page, ids, *, failed=False, held=False, workspace_mismatch=False,
                   delay_refresh=False, preview_workspace_mismatch=False):
    state = {"submitted": [], "polls": 0, "terminal": False, "cancelled": False, "refresh_held": [], "refreshes": 0}
    workspace_id = live_server["db"].active_workspace_id

    def readiness(route):
        selected = route.request.post_data_json["photo_ids"]
        photos = [{"photo_id": pid, "filename": f"selected-{pid}.jpg",
                   "status": "complete" if state["terminal"] and not failed else "incomplete",
                   "reasons": ["missing_subject_mask"], "eligible": state["terminal"] and not failed,
                   "explanation": "Subject analysis complete." if state["terminal"] and not failed else
                   "Subject analysis incomplete — segmentation missing; historical cause unknown.",
                   "required_stages": [] if state["terminal"] and not failed else ["segmentation"],
                   "last_attempt": {"photo_id": pid, "status": "failed", "stage": "segmentation",
                                    "error": "Mask could not be produced"} if failed and state["terminal"] else None,
                   "provenance": {}} for pid in selected]
        route.fulfill(json={"workspace_id": workspace_id + 100 if preview_workspace_mismatch else workspace_id, "count": len(photos),
                            "max_selected_photos": 200, "photos": photos})

    def submit(route):
        state["submitted"].append(route.request.post_data_json)
        route.fulfill(json={"job_id": "selected-analysis-test", "photo_ids": ids[:1]})

    def progress(route):
        state["polls"] += 1
        state["terminal"] = state["cancelled"] or (not held and state["polls"] > 1)
        status = "cancelled" if state["cancelled"] else "completed" if state["terminal"] else "running"
        route.fulfill(json={"status": status, "progress": {"total": 1, "current": int(state["terminal"]),
                                                          "current_file": f"selected-{ids[0]}.jpg", "phase": "Segmentation"},
                            "result": {"photos": [{"photo_id": ids[0], "status": "failed" if failed else "complete"}]}})

    def cancel(route):
        state["cancelled"] = True
        route.fulfill(json={"cancelled": True})

    def refresh(route):
        state["refreshes"] += 1
        response = route.fetch()
        data = response.json()
        data["workspace_id"] = workspace_id + 100 if workspace_mismatch else workspace_id
        if not failed:
            data["results"]["photos"][0]["subject_tenengrad"] = 777
            data["results"]["photos"][0]["analysis_status"] = "complete"
        if delay_refresh:
            state["refresh_held"].append((route, data))
            page.evaluate("document.getElementById('analysisRetryPhotos').dataset.refreshHeld = '1'")
        else:
            route.fulfill(json=data)

    page.route("**/api/photos/analysis-readiness", readiness)
    page.route("**/api/photos/analysis-retry", submit)
    page.route("**/api/jobs/selected-analysis-test", progress)
    page.route("**/api/jobs/selected-analysis-test/cancel", cancel)
    page.route("**/api/pipeline/page-init", refresh)
    return state


def test_retry_previews_exact_selection_and_refreshes_without_losing_staged_flags(live_server, page):
    ids = _open_review(live_server, page)
    state = _install_retry(live_server, page, ids)
    page.evaluate("ids => grmMovePick([ids[1]])", ids)
    page.locator("#grmAnalysisRetryAction").click()
    expect(page.locator("#analysisRetryPhotos li")).to_have_count(1)
    expect(page.locator("#analysisRetryPhotos")).to_contain_text("segmentation missing; historical cause unknown")
    expect(page.locator("#analysisRetryPhotos")).not_to_contain_text(f"selected-{ids[1]}.jpg")
    page.locator("#analysisRetryStart").click()
    expect(page.locator("#analysisRetryStatus")).to_contain_text("1 ready for comparison")
    expect(page.locator("#analysisRetryStatus")).to_contain_text("0 without current subject measurements")
    assert state["submitted"] == [{"photo_ids": [ids[0]], "expected_workspace_id": live_server["db"].active_workspace_id}]
    assert page.evaluate("pid => findPhotoInResults(pid).subject_tenengrad", ids[0]) == 777
    assert page.evaluate("pid => grmState.picks.has(pid)", ids[1])
    assert live_server["db"].photos.get(ids[1])["flag"] == "none"
    expect(page.locator("#analysisRetryStart")).to_be_disabled()


def test_failed_analysis_displays_recorded_error_and_allows_retry(live_server, page):
    ids = _open_review(live_server, page)
    _install_retry(live_server, page, ids, failed=True)
    page.locator("#grmAnalysisRetryAction").click()
    page.locator("#analysisRetryStart").click()
    expect(page.locator("#analysisRetryStatus")).to_contain_text("1 failed")
    expect(page.locator("#analysisRetryPhotos")).to_contain_text("Mask could not be produced")
    expect(page.locator("#analysisRetryStart")).to_be_enabled()
    assert page.evaluate("pid => findPhotoInResults(pid).label", ids[0]) == "REVIEW"


def test_cancelled_analysis_keeps_the_selection_and_allows_another_attempt(live_server, page):
    ids = _open_review(live_server, page)
    state = _install_retry(live_server, page, ids, held=True, failed=True)
    page.locator("#grmAnalysisRetryAction").click()
    page.locator("#analysisRetryStart").click()
    expect(page.locator("#analysisRetryStatus")).to_contain_text("Segmentation")
    page.locator("#analysisRetryCancel").click()
    expect(page.locator("#analysisRetryStatus")).to_contain_text("Analysis cancelled")
    expect(page.locator("#analysisRetryStart")).to_be_enabled()
    assert state["submitted"][0]["photo_ids"] == [ids[0]]


def test_retry_refuses_to_refresh_from_another_workspace(live_server, page):
    ids = _open_review(live_server, page)
    _install_retry(live_server, page, ids, workspace_mismatch=True)
    before = page.evaluate("pid => findPhotoInResults(pid).subject_tenengrad ?? null", ids[0])
    page.locator("#grmAnalysisRetryAction").click()
    page.locator("#analysisRetryStart").click()
    expect(page.locator("#analysisRetryStatus")).to_contain_text("active workspace changed")
    assert page.evaluate("pid => findPhotoInResults(pid).subject_tenengrad ?? null", ids[0]) == before


def test_scope_change_after_preview_cannot_retry_hidden_photos(live_server, page):
    ids = _open_review(live_server, page)
    state = _install_retry(live_server, page, ids)
    page.locator("#grmAnalysisRetryAction").click()
    expect(page.locator("#analysisRetryStart")).to_be_enabled()
    page.evaluate("ids => { closeGroupReview(true); reviewScopeMode = 'folder'; reviewScopePhotoIds = new Set([ids[1]]); }", ids)
    page.locator("#analysisRetryStart").click()
    expect(page.locator("#analysisRetryStatus")).to_contain_text("selection or view changed")
    assert state["submitted"] == []


def test_closing_while_submit_is_pending_does_not_change_job_targets(live_server, page):
    ids = _open_review(live_server, page)
    state = _install_retry(live_server, page, ids)
    pending = []
    page.route("**/api/photos/analysis-retry", lambda route: pending.append(route))
    page.locator("#grmAnalysisRetryAction").click()
    with page.expect_request("**/api/photos/analysis-retry"):
        page.locator("#analysisRetryStart").click()
    page.locator("#analysisRetryClose").click()
    page.evaluate("pid => grmSelect(pid, 'replace')", ids[1])
    page.locator("#grmAnalysisRetryAction").click()
    expect(page.locator("#analysisRetryPhotos")).to_contain_text(f"selected-{ids[0]}.jpg")
    assert len(pending) == 1
    assert pending[0].request.post_data_json["photo_ids"] == [ids[0]]
    pending[0].fulfill(json={"job_id": "selected-analysis-test", "photo_ids": [ids[0]]})
    expect(page.locator("#analysisRetryStatus")).to_contain_text("1 ready for comparison")
    assert state["polls"] >= 2


def test_decision_changed_during_view_refresh_wins_over_the_old_response(live_server, page):
    ids = _open_review(live_server, page)
    state = _install_retry(live_server, page, ids, delay_refresh=True)
    page.locator("#grmAnalysisRetryAction").click()
    page.locator("#analysisRetryStart").click()
    expect(page.locator("#analysisRetryStatus")).to_contain_text("Reloading current analysis")
    expect(page.locator("#analysisRetryPhotos")).to_have_attribute("data-refresh-held", "1")
    # A later user action in this page must win even if the response was read earlier.
    page.evaluate("pid => { const p = findPhotoInResults(pid); p.flag = 'rejected'; p.rating = 2; }", ids[0])
    assert len(state["refresh_held"]) == 1
    route, payload = state["refresh_held"][0]
    route.fulfill(json=payload)
    expect(page.locator("#analysisRetryStatus")).not_to_contain_text("Reloading current analysis")
    assert page.evaluate("pid => findPhotoInResults(pid).flag", ids[0]) == "rejected"
    assert page.evaluate("pid => findPhotoInResults(pid).rating", ids[0]) == 2
    assert page.evaluate("pid => findPhotoInResults(pid).label", ids[0]) == "REJECT"
    assert page.evaluate("pid => findPhotoInResults(pid).subject_tenengrad", ids[0]) == 777


def test_read_only_regrouped_view_can_explain_but_cannot_start_retry(live_server, page):
    ids = _open_review(live_server, page)
    state = _install_retry(live_server, page, ids)
    page.evaluate("reviewScopeRegrouped = true")
    page.locator("#grmAnalysisRetryAction").click()
    expect(page.locator("#analysisRetryPhotos li")).to_have_count(1)
    expect(page.locator("#analysisRetryStatus")).to_contain_text("regrouped view is read-only")
    expect(page.locator("#analysisRetryStart")).to_be_disabled()
    assert state["submitted"] == []


def test_preview_cannot_adopt_another_workspace_for_shared_photos(live_server, page):
    ids = _open_review(live_server, page)
    state = _install_retry(live_server, page, ids, preview_workspace_mismatch=True)
    page.locator("#grmAnalysisRetryAction").click()
    expect(page.locator("#analysisRetryStatus")).to_contain_text("active workspace changed")
    expect(page.locator("#analysisRetryStart")).to_be_disabled()
    assert page.evaluate("reviewAnalysisRetry.workspaceId") == live_server["db"].active_workspace_id
    assert state["submitted"] == []


def test_scope_change_while_submission_pending_does_not_overlay_regrouped_scores(live_server, page):
    ids = _open_review(live_server, page)
    state = _install_retry(live_server, page, ids)
    pending = []
    page.route("**/api/photos/analysis-retry", lambda route: pending.append(route))
    page.locator("#grmAnalysisRetryAction").click()
    with page.expect_request("**/api/photos/analysis-retry"):
        page.locator("#analysisRetryStart").click()
    page.locator("#analysisRetryClose").click()
    page.evaluate("""pid => {
        closeGroupReview(true);
        pipelineResults = structuredClone(pipelineResults);
        reviewScopeRegrouped = true;
        reviewScopeRequestSeq++;
        const photo = findPhotoInResults(pid);
        photo.subject_tenengrad = 999;
        photo.quality_composite = .123;
        photo.label = 'REJECT';
    }""", ids[0])
    pending[0].fulfill(json={"job_id": "selected-analysis-test", "photo_ids": [ids[0]]})
    expect(page.locator("#analysisRetryBanner")).to_contain_text("view changed during analysis")
    assert page.evaluate("pid => findPhotoInResults(pid).subject_tenengrad", ids[0]) == 999
    assert page.evaluate("pid => findPhotoInResults(pid).quality_composite", ids[0]) == .123
    assert page.evaluate("pid => findPhotoInResults(pid).label", ids[0]) == "REJECT"
    assert state["refreshes"] == 0
