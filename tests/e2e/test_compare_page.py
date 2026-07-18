from playwright.sync_api import expect


def test_compare_keyword_conflict_filter_includes_non_top_predictions(live_server, page):
    from labels_fingerprint import TOL_SENTINEL

    db = live_server["db"]
    photo_id = live_server["data"]["photos"][0]
    det_id = db.conn.execute(
        "SELECT id FROM detections WHERE photo_id = ? ORDER BY id LIMIT 1",
        (photo_id,),
    ).fetchone()["id"]
    db.add_prediction(
        detection_id=det_id,
        species="Cooper's Hawk",
        confidence=0.41,
        model="BioCLIP-2",
        category="conflict",
        labels_fingerprint=TOL_SENTINEL,
    )
    db.conn.commit()

    page.goto(f"{live_server['url']}/compare")
    page.locator("#filterRow button", has_text="Keyword vs models").click()

    row = page.locator(f'tr[data-photo-id="{photo_id}"]')
    expect(row).to_be_visible()
    expect(row).to_contain_text("Cooper's Hawk")
    expect(row.locator(".signal-pill.hot")).to_contain_text("Keyword")


def test_compare_page_shows_keyword_workflow(live_server, page):
    page.goto(f"{live_server['url']}/compare")

    expect(page.locator("#summaryGrid")).to_be_visible()
    expect(page.locator("#filterRow")).to_contain_text("Needs review")
    expect(page.locator(".compare-table")).to_be_visible()
    expect(page.locator("th", has_text="Photo")).to_be_visible()
    expect(page.locator("th", has_text="Status")).to_be_visible()
    expect(page.locator("th", has_text="Current keywords")).to_be_visible()
    page.locator("#filterRow button", has_text="All").click()
    expect(page.locator(".keyword-pill.species").first).to_contain_text("Red-tailed Hawk")


def test_compare_page_filters_conflicts_without_crashing(live_server, page):
    page.goto(f"{live_server['url']}/compare")

    expect(page.locator("#summaryGrid")).to_be_visible()
    page.locator("#filterRow button", has_text="Matches").click()

    expect(page.locator("#filterRow .active")).to_contain_text("Matches")


def test_compare_page_exposes_disagreement_filters_and_sorts(live_server, page):
    page.goto(f"{live_server['url']}/compare")

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


def test_compare_page_thumbnail_opens_lightbox(live_server, page):
    page.goto(f"{live_server['url']}/compare")

    page.locator("#filterRow button", has_text="All").click()
    first_row = page.locator(".compare-table tbody tr").first
    expect(first_row).to_be_visible()
    filename = first_row.locator(".photo-name").inner_text()

    first_row.locator(".photo-thumb-button").click()

    expect(page.locator("#lightboxOverlay")).to_have_class("lightbox-overlay active")
    expect(page.locator("#lightboxFilename")).to_have_text(filename)


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
    db.add_prediction(
        second_detection,
        "Cooper's Hawk",
        0.91,
        "BioCLIP-2",
        labels_fingerprint=TOL_SENTINEL,
    )
    db.add_prediction(
        second_detection,
        "Cooper's Hawk",
        0.88,
        "iNat21",
        labels_fingerprint=TOL_SENTINEL,
    )

    page.goto(f"{live_server['url']}/compare")
    page.wait_for_function("() => window.compareData !== null")
    subject_state = page.evaluate(
        """(photoId) => {
          const photo = compareData.photos.find(item => item.photo_id === photoId);
          const assessment = photoSubjectAssessment(photo);
          return {
            subjectCount: assessment.subjects.length,
            categories: assessment.statuses.map(item => item.category),
            species: assessment.signals.map(item => item.consensus_species),
          };
        }""",
        photo_id,
    )
    assert subject_state == {
        "subjectCount": 2,
        "categories": ["match", "additional"],
        "species": ["Red-tailed Hawk", "Cooper's Hawk"],
    }

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
    expect(row.locator(".keyword-pill.species", has_text="Cooper's Hawk")).to_be_visible()

    keyword_names = {item["name"] for item in db.get_photo_keywords(photo_id)}
    assert {"Red-tailed Hawk", "Cooper's Hawk"} <= keyword_names
    statuses = {
        pred["model"]: pred["status"]
        for pred in db.get_predictions(photo_ids=[photo_id])
        if pred["detection_id"] == second_detection
    }
    assert statuses == {"BioCLIP-2": "accepted", "iNat21": "accepted"}
