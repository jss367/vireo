from playwright.sync_api import expect


def test_compare_keyword_conflict_filter_includes_non_top_predictions(live_server, page):
    from labels_fingerprint import TOL_SENTINEL

    db = live_server["db"]
    photo_id = live_server["data"]["photos"][0]
    det_id = db.conn.execute(
        "SELECT id FROM detections WHERE photo_id = ? ORDER BY id LIMIT 1",
        (photo_id,),
    ).fetchone()["id"]
    db.predictions.add(
        detection_id=det_id,
        species="Cooper's Hawk",
        confidence=0.41,
        model="BioCLIP-2",
        category="conflict",
        labels_fingerprint=TOL_SENTINEL,
    )
    db.conn.commit()

    page.goto(f"{live_server['url']}/id-conflicts")
    page.locator("#filterRow button", has_text="Keyword vs models").click()

    row = page.locator(f'tr[data-photo-id="{photo_id}"]')
    expect(row).to_be_visible()
    expect(row).to_contain_text("Cooper's Hawk")
    expect(row.locator(".signal-pill.hot")).to_contain_text("vs keyword")


def test_compare_page_shows_keyword_workflow(live_server, page):
    page.goto(f"{live_server['url']}/id-conflicts")

    expect(page.locator("#summaryGrid")).to_be_visible()
    expect(page.locator("#filterRow")).to_contain_text("Needs review")
    expect(page.locator(".compare-table")).to_be_visible()
    expect(page.locator("th", has_text="Photo")).to_be_visible()
    expect(page.locator("th", has_text="Status")).to_be_visible()
    expect(page.locator("th", has_text="Current keywords")).to_be_visible()
    page.locator("#filterRow button", has_text="All").click()
    expect(page.locator(".keyword-pill.species").first).to_contain_text("Red-tailed Hawk")


def test_compare_page_filters_conflicts_without_crashing(live_server, page):
    page.goto(f"{live_server['url']}/id-conflicts")

    expect(page.locator("#summaryGrid")).to_be_visible()
    page.locator("#filterRow button", has_text="Matches").click()

    expect(page.locator("#filterRow .active")).to_contain_text("Matches")


def test_compare_page_exposes_disagreement_filters_and_sorts(live_server, page):
    page.goto(f"{live_server['url']}/id-conflicts")

    expect(page.locator("#sortRow")).to_be_visible()
    expect(page.locator("#excludeRow")).to_be_visible()
    expect(page.locator("#filterRow")).to_contain_text("Models disagree")
    expect(page.locator("#filterRow")).to_contain_text("Keyword vs models")
    expect(page.locator("#excludeRow")).to_contain_text("Hide rejects")
    expect(page.locator("#excludeRow")).to_contain_text("Hide picks")

    page.locator("#sortRow button", has_text="Model disagreement").click()
    expect(page.locator("#sortRow .active")).to_contain_text("Model disagreement")

    page.locator("#excludeRow button", has_text="Hide rejects").click()
    expect(page.locator("#excludeRow .active")).to_contain_text("Hide rejects")

    page.locator("#filterRow button", has_text="Keyword vs models").click()
    expect(page.locator("#filterRow .active")).to_contain_text("Keyword vs models")


def test_compare_sort_persists_across_navigation(live_server, page):
    url = live_server["url"]
    page.goto(f"{url}/id-conflicts")
    expect(page.locator("#sortRow")).to_be_visible()

    page.locator("#sortRow button", has_text="Filename").click()
    page.goto(f"{url}/")
    page.goto(f"{url}/id-conflicts")

    expect(page.locator("#sortRow .active")).to_contain_text("Filename")
    expect(page.locator("#sortMode")).to_have_value("filename")


def test_compare_page_thumbnail_opens_lightbox(live_server, page):
    page.goto(f"{live_server['url']}/id-conflicts")

    page.locator("#filterRow button", has_text="All").click()
    first_row = page.locator(".compare-table tbody tr").first
    expect(first_row).to_be_visible()
    filename = first_row.locator(".photo-name").inner_text()

    first_row.locator(".photo-thumb-button").click()

    expect(page.locator("#lightboxOverlay")).to_have_class("lightbox-overlay active")
    expect(page.locator("#lightboxFilename")).to_have_text(filename)


def test_compare_reject_updates_in_place_without_reloading_collection(
    live_server, page
):
    """Reject refreshes one photo, never the complete collection."""
    from urllib.parse import parse_qs, urlparse

    compare_requests = []
    page.on(
        "request",
        lambda request: compare_requests.append(request.url)
        if "/api/predictions/compare?" in request.url
        else None,
    )
    page.goto(f"{live_server['url']}/id-conflicts")
    page.wait_for_function("() => window.compareData !== null")

    page.locator("#filterRow button", has_text="All").click()
    row = page.locator(".compare-table tbody tr").first
    expect(row).to_be_visible()
    photo_id = row.get_attribute("data-photo-id")
    compare_requests.clear()

    # The scoped refresh is only sent once /reject has returned, and the page
    # patches the pill optimistically before that. Waiting on the pill alone
    # would let the count below be read before the refresh was ever issued, so
    # wait for the refresh request itself.
    with page.expect_response("**/reject") as response_info, page.expect_response(
        lambda r: "/api/predictions/compare?" in r.url and "refresh_photo_id=" in r.url
    ) as refresh_info:
        row.get_by_role("button", name="Reject", exact=True).first.click()
    assert response_info.value.ok
    assert refresh_info.value.ok

    row = page.locator(f'tr[data-photo-id="{photo_id}"]')
    expect(row.locator(".status-pill.rejected")).to_be_visible()
    assert len(compare_requests) == 1
    query = parse_qs(urlparse(compare_requests[0]).query)
    # The one request names the photo it changed: the stored comparison
    # rebuilds that row, and nothing else in the collection is touched.
    assert query["refresh_photo_id"] == [photo_id]
    assert "photo_id" not in query


def test_compare_accept_refreshes_only_the_changed_photo(live_server, page):
    """Keyword writes refresh their taxonomy comparison, not the collection."""
    from urllib.parse import parse_qs, urlparse

    photo_id = live_server["data"]["photos"][1]
    compare_requests = []
    page.on(
        "request",
        lambda request: compare_requests.append(request.url)
        if "/api/predictions/compare?" in request.url
        else None,
    )
    page.goto(f"{live_server['url']}/id-conflicts")
    page.wait_for_function("() => window.compareData !== null")

    page.locator("#filterRow button", has_text="All").click()
    row = page.locator(f'tr[data-photo-id="{photo_id}"]')
    expect(row).to_be_visible()
    compare_requests.clear()

    # Same ordering as the reject case: the keyword pill can render from the
    # local patch, so the refresh request has to be waited on separately.
    with page.expect_response("**/accept") as response_info, page.expect_response(
        lambda r: "/api/predictions/compare?" in r.url and "refresh_photo_id=" in r.url
    ) as refresh_info:
        row.get_by_role("button", name="Add keyword", exact=True).click()
    assert response_info.value.ok
    assert refresh_info.value.ok

    expect(row.locator(".keyword-pill.species")).to_contain_text(
        "Red-tailed Hawk"
    )
    assert len(compare_requests) == 1
    query = parse_qs(urlparse(compare_requests[0]).query)
    assert str(photo_id) in query["refresh_photo_id"]
    assert "photo_id" not in query


def test_compare_serializes_scoped_decision_refreshes(live_server, page):
    """An older scoped response cannot merge after a newer decision."""
    page.goto(f"{live_server['url']}/id-conflicts")
    page.wait_for_function("() => window.compareData !== null")

    result = page.evaluate(
        """async () => {
          var calls = [];
          var releaseFirst;
          var firstGate = new Promise(function(resolve) {
            releaseFirst = resolve;
          });
          var originalRefresh = refreshComparisonPhotos;
          refreshComparisonPhotos = async function(ids) {
            calls.push(ids[0]);
            if (ids[0] === 1) await firstGate;
          };
          try {
            var first = refreshDecisionPhotos([1]);
            await new Promise(function(resolve) { setTimeout(resolve, 0); });
            var second = refreshDecisionPhotos([2]);
            await new Promise(function(resolve) { setTimeout(resolve, 0); });
            var beforeRelease = calls.slice();
            releaseFirst();
            await Promise.all([first, second]);
            return {beforeRelease: beforeRelease, afterRelease: calls};
          } finally {
            refreshComparisonPhotos = originalRefresh;
          }
        }"""
    )

    assert result == {"beforeRelease": [1], "afterRelease": [1, 2]}


def test_compare_discards_queued_refresh_after_collection_load(live_server, page):
    """Queued work stays bound to the collection that originated it."""
    page.goto(f"{live_server['url']}/id-conflicts")
    page.wait_for_function("() => window.compareData !== null")

    result = page.evaluate(
        """async () => {
          var calls = [];
          var releaseFirst;
          var firstGate = new Promise(function(resolve) {
            releaseFirst = resolve;
          });
          var originalRefresh = refreshComparisonPhotos;
          refreshComparisonPhotos = async function(ids) {
            calls.push(ids[0]);
            if (ids[0] === 1) await firstGate;
          };
          try {
            var first = refreshDecisionPhotos([1]);
            await new Promise(function(resolve) { setTimeout(resolve, 0); });
            var queued = refreshDecisionPhotos([2]);
            loadingSeq++;
            releaseFirst();
            await Promise.all([first, queued]);
            return calls;
          } finally {
            refreshComparisonPhotos = originalRefresh;
          }
        }"""
    )

    assert result == [1]


def test_compare_scoped_refresh_exits_empty_needs_review_filter(live_server, page):
    """Resolving the final pending row reveals the remaining collection."""
    page.goto(f"{live_server['url']}/id-conflicts")
    page.wait_for_function("() => window.compareData !== null")

    result = page.evaluate(
        """async () => {
          var originalFetch = jsonFetch;
          // Answer the decision refresh the way the server would once the
          // last pending row is settled: nothing left needing review.
          var settled = JSON.parse(JSON.stringify(compareData));
          settled.summary.needs_review = 0;
          settled.photos = [];
          settled.total = 0;
          jsonFetch = async function() { return settled; };
          activeFilter = 'needs_review';
          try {
            await refreshComparisonPhotos(compareData.photos.map(function(photo) {
              return photo.photo_id;
            }));
            return {
              filter: activeFilter,
              needsReview: effectiveSummary().needs_review,
            };
          } finally {
            jsonFetch = originalFetch;
          }
        }"""
    )

    assert result == {"filter": "all", "needsReview": 0}


def test_compare_decision_refresh_keeps_its_origin_scope(live_server, page):
    """A collection load during the POST supersedes its later refresh."""
    page.goto(f"{live_server['url']}/id-conflicts")
    page.wait_for_function("() => window.compareData !== null")

    result = page.evaluate(
        """async () => {
          var predId = compareData.photos[0].predictions[
            Object.keys(compareData.photos[0].predictions)[0]
          ][0].id;
          var originalFetch = jsonFetch;
          var originalRefresh = refreshComparisonPhotos;
          var originalLoad = loadComparison;
          var releasePost;
          var postGate = new Promise(function(resolve) { releasePost = resolve; });
          var refreshCalls = [];
          var fullLoads = 0;
          jsonFetch = async function(url) {
            if (url.indexOf('/api/predictions/') === 0) {
              await postGate;
              return {ok: true};
            }
            return originalFetch(url);
          };
          refreshComparisonPhotos = async function(ids) {
            refreshCalls.push(ids);
          };
          loadComparison = async function() { fullLoads++; };
          try {
            var decision = predictionAction(predId, 'reviewed');
            await new Promise(function(resolve) { setTimeout(resolve, 0); });
            loadingSeq++;
            releasePost();
            await decision;
            return {refreshCalls: refreshCalls, fullLoads: fullLoads};
          } finally {
            jsonFetch = originalFetch;
            refreshComparisonPhotos = originalRefresh;
            loadComparison = originalLoad;
          }
        }"""
    )

    assert result == {"refreshCalls": [], "fullLoads": 1}


def test_compare_grouped_409_performs_full_reload(live_server, page):
    """An external grouped decision cannot be reconciled from one photo."""
    page.goto(f"{live_server['url']}/id-conflicts")
    page.wait_for_function("() => window.compareData !== null")

    result = page.evaluate(
        """async () => {
          var predId = compareData.photos[0].predictions[
            Object.keys(compareData.photos[0].predictions)[0]
          ][0].id;
          var originalFetch = jsonFetch;
          var originalLoad = loadComparison;
          var originalRefresh = refreshDecisionPhotos;
          var fullLoads = 0;
          var targetedRefreshes = 0;
          jsonFetch = async function() {
            var error = new Error('prediction already accepted');
            error.status = 409;
            error.answered = true;
            throw error;
          };
          loadComparison = async function() { fullLoads++; };
          refreshDecisionPhotos = async function() { targetedRefreshes++; };
          try {
            await predictionAction(predId, 'accept');
            return {fullLoads: fullLoads, targetedRefreshes: targetedRefreshes};
          } finally {
            jsonFetch = originalFetch;
            loadComparison = originalLoad;
            refreshDecisionPhotos = originalRefresh;
          }
        }"""
    )

    assert result == {"fullLoads": 1, "targetedRefreshes": 0}


def test_compare_treats_second_detected_species_as_additional(live_server, page):
    """A second subject is additional information, not a tag conflict."""
    from labels_fingerprint import TOL_SENTINEL

    db = live_server["db"]
    photo_id = live_server["data"]["photos"][0]
    second_detection = db.save_detections(
        photo_id,
        [{
            "box": {"x": 0.58, "y": 0.35, "w": 0.22, "h": 0.3},
            "confidence": 0.72,
            "category": "animal",
        }],
        detector_model="test-detector-secondary",
    )[0]
    db.predictions.add(
        second_detection,
        "Cooper's Hawk",
        0.91,
        "BioCLIP-2",
        labels_fingerprint=TOL_SENTINEL,
    )
    db.predictions.add(
        second_detection,
        "Cooper's Hawk",
        0.88,
        "iNat21",
        labels_fingerprint=TOL_SENTINEL,
    )

    page.goto(f"{live_server['url']}/compare")
    page.wait_for_function("() => window.compareData !== null")

    page.locator("#filterRow button", has_text="All").click()
    subject_summaries = page.locator(
        f'tr[data-photo-id="{photo_id}"] .subject-summary'
    )
    expect(subject_summaries).to_have_count(2)
    expect(subject_summaries.nth(0)).to_contain_text("Red-tailed Hawk")
    expect(subject_summaries.nth(0)).to_contain_text("Match")
    expect(subject_summaries.nth(1)).to_contain_text("Cooper's Hawk")
    expect(subject_summaries.nth(1)).to_contain_text("Additional species suggested")

    page.locator("#filterRow button", has_text="Models disagree").click()
    expect(page.locator(f'tr[data-photo-id="{photo_id}"]')).to_have_count(0)

    page.locator("#filterRow button", has_text="Additional species").click()

    row = page.locator(f'tr[data-photo-id="{photo_id}"]')
    expect(row).to_be_visible()
    expect(row).to_contain_text("2 subjects")
    expect(row).to_contain_text("Additional species suggested")
    expect(row).to_contain_text("Cooper's Hawk")
    expect(row.locator('input[type="checkbox"]')).to_be_disabled()
    expect(row.get_by_role("button", name="Replace keyword")).to_have_count(0)

    page.locator("#filterRow button", has_text="Keyword vs models").click()
    expect(row).to_have_count(0)

    page.locator("#filterRow button", has_text="Additional species").click()
    row = page.locator(f'tr[data-photo-id="{photo_id}"]')
    with page.expect_response("**/accept-subject") as response_info:
        row.get_by_role("button", name="Add additional species").first.click()
    assert response_info.value.ok
    page.locator("#filterRow button", has_text="All").click()
    row = page.locator(f'tr[data-photo-id="{photo_id}"]')
    expect(row.locator(".keyword-pill.species", has_text="Cooper's Hawk")).to_be_visible()

    keyword_names = {item["name"] for item in db.photo_keywords.get_for_photo(photo_id)}
    assert {"Red-tailed Hawk", "Cooper's Hawk"} <= keyword_names
    statuses = {
        pred["model"]: pred["status"]
        for pred in db.predictions.get_rows(photo_ids=[photo_id])
        if pred["detection_id"] == second_detection
    }
    assert statuses == {"BioCLIP-2": "accepted", "iNat21": "accepted"}


def test_compare_missing_prediction_filter_includes_subjectless_photos(live_server, page):
    """A photo with no compare subjects at all (no qualifying detection and
    no full-image prediction) still falls back to ``missing_prediction`` in
    ``photoReviewStatus()``. The Missing predictions filter and pill count
    must include it — otherwise subjectless missing-prediction photos
    disappear from the very filter meant to surface them.
    """
    from PIL import Image

    db = live_server["db"]
    folder_id = live_server["data"]["folders"][0]
    subjectless_pid = db.add_photo(
        folder_id=folder_id, filename="subjectless.jpg", extension=".jpg",
        file_size=1000, file_mtime=1.0, timestamp="2024-03-10T09:00:00",
    )
    thumb_dir = live_server["app"].config["THUMB_CACHE_DIR"]
    Image.new("RGB", (100, 100), color="blue").save(
        f"{thumb_dir}/{subjectless_pid}.jpg"
    )

    page.goto(f"{live_server['url']}/compare")
    page.wait_for_function("() => window.compareData !== null")

    summary_missing = page.evaluate("() => effectiveSummary().missing_predictions")
    assert summary_missing >= 1

    page.locator("#filterRow button", has_text="Missing predictions").click()
    row = page.locator(f'tr[data-photo-id="{subjectless_pid}"]')
    expect(row).to_be_visible()
    expect(row.locator("td:nth-child(3) .category-pill")).to_have_text(
        "Missing prediction"
    )


def test_compare_row_status_surfaces_unclassified_over_pending_match(
    live_server, page
):
    """When a multi-subject photo has one pending-match subject and another
    detected-but-unclassified subject, the row status must render/sort as
    ``unclassified`` — the higher-priority category — not as the pending
    match. Regression for a bug where ``photoReviewStatus()`` filtered
    candidates by ``status.needs_review`` first, dropping the unclassified
    subject (which has no predictions and therefore no pending state) before
    ``CATEGORY_ORDER`` could apply.
    """
    db = live_server["db"]
    photo_id = live_server["data"]["photos"][0]
    # Add a second detection with no predictions — an unclassified subject.
    db.save_detections(
        photo_id,
        [{
            "box": {"x": 0.6, "y": 0.4, "w": 0.25, "h": 0.3},
            "confidence": 0.75,
            "category": "animal",
        }],
        detector_model="test-detector-secondary",
    )

    page.goto(f"{live_server['url']}/compare")
    page.wait_for_function("() => window.compareData !== null")
    page.locator("#filterRow button", has_text="All").click()

    row = page.locator(f'tr[data-photo-id="{photo_id}"]')
    expect(row).to_be_visible()
    # Both subjects are present: the original with a pending match and the
    # new detection with no predictions (unclassified).
    summaries = row.locator(".subject-summary")
    expect(summaries).to_have_count(2)
    expect(summaries.nth(0)).to_contain_text("Match")
    expect(summaries.nth(1)).to_contain_text("Unclassified subject")
    # The row surfaces the unclassified subject rather than the pending match.
    expect(row.locator("td:nth-child(3) .category-pill")).to_have_text(
        "Unclassified subject"
    )


def test_compare_full_load_lands_on_the_collection_when_nothing_needs_review(
    live_server, page
):
    """The page opens on the Needs review queue. When that queue is empty it
    must show the collection instead of an empty table — the filter the page
    chose for itself has to follow the data it was chosen from.
    """
    from labels_fingerprint import TOL_SENTINEL

    db = live_server["db"]
    photo_id = live_server["data"]["photos"][0]

    page.goto(f"{live_server['url']}/compare")
    page.wait_for_function("() => window.compareData !== null")

    # Settle every pending row. The predictions table has no ``status``
    # column — per-workspace review state lives in ``prediction_review``,
    # where an absent row means pending — so accept them explicitly.
    pred_ids = [
        row["id"]
        for row in db.conn.execute("SELECT id FROM predictions").fetchall()
    ]
    for pred_id in pred_ids:
        db.update_prediction_status(pred_id, "accepted", _commit=False)
    db.conn.commit()
    # Seed one settled row so the photo does not collapse to
    # missing_prediction once the pending ones are gone.
    det_id = db.conn.execute(
        "SELECT id FROM detections WHERE photo_id = ? ORDER BY id LIMIT 1",
        (photo_id,),
    ).fetchone()["id"]
    db.predictions.add(
        detection_id=det_id,
        species="Red-tailed Hawk",
        confidence=0.95,
        model="BioCLIP-2",
        category="match",
        status="accepted",
        labels_fingerprint=TOL_SENTINEL,
    )
    db.conn.commit()

    page.evaluate("() => { window.activeFilter = 'needs_review'; }")
    page.evaluate("async () => { await loadComparison(); }")

    result = page.evaluate(
        """() => ({
          filter: window.activeFilter,
          needsReview: effectiveSummary().needs_review,
        })"""
    )
    assert result["needsReview"] == 0
    assert result["filter"] == "all"
    expect(page.locator("#filterRow .active")).to_contain_text("All")


def test_compare_initial_filter_ignores_an_unavailable_model_assessment(
    live_server, page
):
    """A collection switch that drops the selected model must not let the
    dropped model's empty queue decide the filter. The response was assessed
    under a model this collection does not carry, so its ``needs_review: 0``
    says nothing about the collection; the corrected re-query the reset starts
    is the one entitled to fall back to All.
    """
    page.goto(f"{live_server['url']}/compare")
    page.wait_for_function("() => window.compareData !== null")

    result = page.evaluate(
        """async () => {
          // Select a model the next response will not list, the way a
          // collection switch leaves a stale specific model selected.
          var modelA = document.getElementById('modelA');
          modelA.innerHTML += '<option value="Ghost-Model">Ghost-Model</option>';
          modelA.value = 'Ghost-Model';

          var models = (compareData.models || []).filter(function(name) {
            return name !== 'Ghost-Model';
          });
          var stale = JSON.parse(JSON.stringify(compareData));
          stale.models = models;
          stale.visible_models = ['Ghost-Model'];
          stale.summary.needs_review = 0;
          stale.photos = [];
          stale.total = 0;
          var corrected = JSON.parse(JSON.stringify(compareData));
          corrected.models = models;
          corrected.visible_models = models;
          corrected.summary.needs_review = 3;

          var originalFetch = jsonFetch;
          var calls = [];
          jsonFetch = async function(url) {
            calls.push(url);
            return calls.length === 1 ? stale : corrected;
          };
          activeFilter = 'needs_review';
          try {
            await fetchRows({initial: true, page: 1});
            // The corrected re-query is started from the response handler and
            // not awaited, so let it settle before reading the outcome.
            for (var i = 0; i < 50 && calls.length < 2; i++) {
              await new Promise(function(resolve) { setTimeout(resolve, 0); });
            }
            await new Promise(function(resolve) { setTimeout(resolve, 20); });
            return {
              filter: activeFilter,
              calls: calls.length,
              needsReview: (compareData.summary || {}).needs_review,
              selectedModel: document.getElementById('modelA').value,
            };
          } finally {
            jsonFetch = originalFetch;
          }
        }"""
    )

    # Two requests: the stale one and its correction. A third would mean the
    # stale assessment had already switched the filter to All.
    assert result == {
        "filter": "needs_review",
        "calls": 2,
        "needsReview": 3,
        "selectedModel": "all",
    }
    expect(page.locator("#filterRow .active")).to_contain_text("Needs review")


def test_compare_pages_the_queue_instead_of_rendering_the_collection(
    live_server, page
):
    """The table is a page of the queue, not the whole thing.

    A catalog-sized collection used to be rendered in one go — tens of
    thousands of rows in a single table — which is what made this page
    unusable. The pager has to say where in the queue the user is, and
    the table has to hold only that page.
    """
    from PIL import Image

    db = live_server["db"]
    folder_id = live_server["data"]["folders"][0]
    thumb_dir = live_server["app"].config["THUMB_CACHE_DIR"]
    extra_count = 60
    for index in range(extra_count):
        pid = db.add_photo(
            folder_id=folder_id, filename=f"paged{index:02d}.jpg",
            extension=".jpg", file_size=1000, file_mtime=1.0,
            timestamp=f"2024-04-01T09:{index:02d}:00",
        )
        Image.new("RGB", (60, 60), color="green").save(f"{thumb_dir}/{pid}.jpg")
    db.conn.commit()

    page.goto(f"{live_server['url']}/id-conflicts")
    page.wait_for_function("() => window.compareData !== null")
    page.locator("#filterRow button", has_text="All").click()
    total = len(live_server["data"]["photos"]) + extra_count
    # Controls fetch a new page asynchronously. Wait for the rendered result
    # before touching the next control or reading its total. Use a collection
    # larger than both page sizes so rendering everything cannot pass.
    rows = page.locator(".compare-table tbody tr")
    for per_page in (30, 60, 30):
        page.select_option("#pagerPerPage", str(per_page))
        expect(page.locator("#pagerSummary")).to_contain_text(
            f"1–{per_page} of {total}"
        )
        expect(rows).to_have_count(per_page)

    first_page_ids = page.evaluate(
        "() => compareRows.map(function(p) { return p.photo_id; })"
    )
    assert len(first_page_ids) <= 30


def test_compare_next_page_shows_different_rows(live_server, page):
    from PIL import Image

    db = live_server["db"]
    folder_id = live_server["data"]["folders"][0]
    thumb_dir = live_server["app"].config["THUMB_CACHE_DIR"]
    for index in range(4):
        pid = db.add_photo(
            folder_id=folder_id, filename=f"next{index:02d}.jpg",
            extension=".jpg", file_size=1000, file_mtime=1.0,
            timestamp=f"2024-05-{index + 1:02d}T09:00:00",
        )
        Image.new("RGB", (60, 60), color="green").save(f"{thumb_dir}/{pid}.jpg")
    db.conn.commit()

    page.goto(f"{live_server['url']}/id-conflicts")
    page.wait_for_function("() => window.compareData !== null")
    page.locator("#filterRow button", has_text="All").click()
    page.evaluate("async () => { comparePerPage = 2; await reloadRows(); }")

    expect(page.locator("#pagerSummary")).to_contain_text("page 1 of")
    first = page.evaluate("() => compareRows.map(p => p.photo_id)")
    assert len(first) == 2

    page.locator("#pagerNext").click()
    expect(page.locator("#pagerSummary")).to_contain_text("page 2 of")
    second = page.evaluate("() => compareRows.map(p => p.photo_id)")

    assert set(first).isdisjoint(second)
    expect(page.locator("#pagerPrev")).to_be_enabled()


def test_compare_controls_stay_reachable_while_scrolling(live_server, page):
    """Filter chips, pager and batch actions follow the list down the page.

    Scrolling past them used to strand the user mid-queue with no way to
    change what they were looking at.
    """
    page.goto(f"{live_server['url']}/id-conflicts")
    page.wait_for_function("() => window.compareData !== null")

    position = page.evaluate(
        "() => getComputedStyle(document.getElementById('stickyControls')).position"
    )
    assert position == "sticky"

    page.mouse.wheel(0, 2000)
    expect(page.locator("#filterRow")).to_be_in_viewport()
    expect(page.locator("#pagerRow")).to_be_in_viewport()


def test_compare_search_narrows_the_listed_rows(live_server, page):
    page.goto(f"{live_server['url']}/id-conflicts")
    page.wait_for_function("() => window.compareData !== null")
    page.locator("#filterRow button", has_text="All").click()

    before = page.evaluate("() => compareTotal")
    page.fill("#compareSearch", "Red-tailed")
    page.wait_for_function(
        "(before) => compareTotal < before", arg=before,
    )

    rows = page.locator(".compare-table tbody tr")
    expect(rows.first).to_contain_text("Red-tailed Hawk")


def test_compare_control_query_waits_for_an_in_flight_decision_refresh(
    live_server, page,
):
    """A filter touched mid-decision reads the comparison the decision made.

    Without the ordering the control query reads the stored comparison before
    the refresh has patched it, then wins the staleness guard with its newer
    sequence number and paints pre-decision counts.
    """
    page.goto(f"{live_server['url']}/id-conflicts")
    page.wait_for_function("() => window.compareData !== null")

    result = page.evaluate(
        """async () => {
          var baseline = JSON.parse(JSON.stringify(compareData));
          var photoId = baseline.photos[0].photo_id;
          var originalFetch = jsonFetch;
          var calls = [];
          var releaseRefresh;
          var refreshGate = new Promise(function(resolve) {
            releaseRefresh = resolve;
          });
          // Stands in for the server's stored comparison: the decision only
          // shows up in the counts once its refresh has been answered.
          var serverNeedsReview = 7;
          jsonFetch = async function(url) {
            var isRefresh = url.indexOf('refresh_photo_id') >= 0;
            calls.push(isRefresh ? 'refresh' : 'control');
            if (isRefresh) {
              await refreshGate;
              serverNeedsReview = 3;
            }
            var body = JSON.parse(JSON.stringify(baseline));
            body.summary.needs_review = serverNeedsReview;
            return body;
          };
          try {
            var refresh = refreshDecisionPhotos([photoId]);
            await new Promise(function(resolve) { setTimeout(resolve, 0); });
            // Every user control -- filter, sort, search, threshold, per-page
            // -- re-queries through reloadRows.
            activeFilter = 'all';
            var control = reloadRows();
            await new Promise(function(resolve) { setTimeout(resolve, 0); });
            var beforeRelease = calls.slice();
            releaseRefresh();
            await refresh;
            await control;
            return {
              beforeRelease: beforeRelease,
              afterRelease: calls,
              needsReview: effectiveSummary().needs_review,
            };
          } finally {
            jsonFetch = originalFetch;
          }
        }"""
    )

    assert result == {
        "beforeRelease": ["refresh"],
        "afterRelease": ["refresh", "control"],
        "needsReview": 3,
    }
