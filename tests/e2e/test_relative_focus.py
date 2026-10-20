"""The reviewer can inspect focus evidence and identify legacy uncertainty."""

import json
from pathlib import Path

import pytest
from playwright.sync_api import expect

from e2e.test_pipeline_review_group_reject import _write_grouped_pipeline_cache


@pytest.mark.parametrize("basis", ["eye", "subject", "legacy"])
def test_focus_evidence_is_visible_in_group_and_legacy_inspectors(live_server, page, basis):
    db = live_server["db"]
    ids = live_server["data"]["photos"][:4]
    _write_grouped_pipeline_cache(live_server, ids)
    path = Path(db._db_path).parent / f"pipeline_results_ws{db.active_workspace_id}.json"
    cache = json.loads(path.read_text())
    for photo in cache["photos"]:
        photo.update(focus_score=.125, eye_tenengrad=1229.57, subject_tenengrad=32813.38,
                     bg_tenengrad=6057.47)
        if basis != "legacy":
            photo["focus_evidence"] = {
                "basis": basis, "measurement": 1229.57 if basis == "eye" else 32813.38,
                "background_measurement": None if basis == "eye" else 6057.47,
                "percentile": .125, "relative_score": .125, "cohort_key": basis,
                "cohort_size": 4, "cohort_scope": "encounter", "absolute_status": "not_validated",
                "method": "eye_percentile" if basis == "eye" else "subject_percentile_background_ratio",
            }
    enc = cache["encounters"][0]
    enc["focus_cohorts"] = {basis: {"scope": "encounter", "basis": basis,
                                    "size": 4, "photo_ids": ids, "unidentified_count": 0}}
    if basis == "legacy":
        enc["bursts"] = []  # openInspect falls back to the flat-cache inspector.
        cache["photos"][0]["reject_reasons"] = ["out_of_focus (F=.125 < .35)"]
    path.write_text(json.dumps(cache))
    page.goto(f"{live_server['url']}/pipeline/review")
    expect(page.locator(".photo-card").first).to_be_visible()
    page.evaluate("id => openInspect(id)", ids[0])
    evidence = page.locator(".relative-focus-evidence")
    expect(evidence).to_be_visible()
    expect(evidence).to_contain_text("Absolute sharpness: unvalidated")
    expect(evidence).to_contain_text("exposure", ignore_case=True)
    if basis == "legacy":
        expect(evidence).to_contain_text("comparison group were not recorded")
        expect(page.locator("#inspectPanel")).to_contain_text("absolute defocus was not validated")
    else:
        expect(evidence).to_contain_text("12.50% among 4 photos")
        expect(evidence).to_contain_text("Eye window" if basis == "eye" else "Subject mask (multiple scales)")
        expect(evidence).to_contain_text("1229.57" if basis == "eye" else "32813.38")
        if basis == "subject":
            expect(evidence).to_contain_text("70% subject percentile + 30%")
            expect(evidence).to_contain_text("6057.47")
        evidence.locator("summary").click()
        expect(evidence).to_contain_text("hawk1.jpg")
        expect(evidence).to_contain_text("robin1.jpg")  # Cohort covers the encounter, not just the burst.


def test_misses_focus_category_explains_uncalibrated_review_evidence(live_server, page):
    pid = live_server["data"]["photos"][0]
    page.route("**/api/misses", lambda route: route.fulfill(json={
        "no_subject": [], "clipped": [], "oof": [{"id": pid, "filename": "candidate.jpg",
            "subject_tenengrad": 5., "bg_tenengrad": 20.}],
    }))
    page.goto(f"{live_server['url']}/misses")
    section = page.locator("#missesSection-oof")
    expect(section).to_contain_text("Focus needs review")
    expect(section).to_contain_text("absolute defocus is unvalidated")
    expect(section).to_contain_text("Subject/background gradients: 0.25")
