"""Selected retry refresh preserves group identity and species-only semantics."""
from copy import deepcopy

import pytest
from services.saved_review import refresh_selected_suggestions


def photo(pid, **values):
    return {"id": pid, "filename": f"duck-{pid}.jpg", "flag": "none", "rating": 0,
            "label": "REVIEW", "species_top5": [], "confirmed_species_identities": [],
            "confirmed_species_list": [], **values}


def group(ids, name):
    return {"photo_ids": ids, "name": name, "photo_count": len(ids), "burst_count": 1,
            "species_confirmed": True, "confirmed_species": name,
            "confirmed_species_list": [name], "bursts": [{"photo_ids": ids,
                "species_override": {"confirmed": True, "species_list": [name]}}]}


def stub_features(monkeypatch, current):
    import pipeline
    seen = []

    def load(db, **kwargs):
        seen.append(kwargs["photo_ids"])
        return deepcopy(current)

    monkeypatch.setattr(pipeline, "load_photo_features", load)
    return seen


def stub_triage(monkeypatch):
    import pipeline

    def triage(encounters, **kwargs):
        photos = []
        for encounter in encounters:
            ids = [p["id"] for p in encounter["photos"]]
            encounter["focus_cohorts"] = [{"photo_ids": ids, "refreshed": True}]
            for current in encounter["photos"]:
                current.update(label="REVIEW", quality_composite=.5)
                photos.append(current)
        return encounters, photos

    monkeypatch.setattr(pipeline, "run_triage", triage)


def test_invisible_first_encounter_does_not_receive_later_group(monkeypatch):
    saved = {"photos": [photo(pid) for pid in [1, 2, 3, 4]],
             "encounters": [group([1], "First"), group([2, 3], "Second"), group([4], "Unrelated")],
             "summary": {}}
    before_first = deepcopy(saved["encounters"][0])
    before_unrelated = deepcopy(saved["encounters"][2])
    seen = stub_features(monkeypatch, [photo(2), photo(3)])
    stub_triage(monkeypatch)
    refresh_selected_suggestions(object(), saved, [1, 2], {})
    assert seen == [[1, 2, 3]]
    assert saved["encounters"][0] == before_first
    assert saved["encounters"][2] == before_unrelated
    second = saved["encounters"][1]
    assert second["photo_ids"] == [2, 3] and second["name"] == "Second"
    assert second["focus_cohorts"] == [{"photo_ids": [2, 3], "refreshed": True}]
    assert second["bursts"][0]["species_override"]["species_list"] == ["Second"]
    assert [pid for encounter in saved["encounters"] for pid in encounter["photo_ids"]] == [1, 2, 3, 4]


def test_new_remainder_encounter_does_not_replace_dropped_original(monkeypatch):
    saved = {"photos": [photo(1), photo(5)], "encounters": [group([1], "Original")], "summary": {}}
    before = deepcopy(saved["encounters"])
    stub_features(monkeypatch, [photo(5, mask_path="new-mask.png")])
    stub_triage(monkeypatch)
    refresh_selected_suggestions(object(), saved, [1, 5], {})
    assert saved["encounters"] == before
    assert next(p for p in saved["photos"] if p["id"] == 5)["mask_path"] == "new-mask.png"


@pytest.mark.parametrize("complete", [True, False])
def test_species_mode_refreshes_analysis_without_quality_triage(monkeypatch, complete):
    import pipeline
    from subject_readiness import REQUIRED_QUALITY_FIELDS

    old = photo(1, mask_path="old.png", analysis_status="not_run", analysis_reasons=["old"],
                quality_composite=.9, focus_score=.8, review_note="Keep annotation")
    saved = {"review_mode": "species", "photos": [old, photo(2)],
             "encounters": [group([1], "Selected"), group([2], "Unrelated")], "summary": {}}
    original_group = deepcopy(saved["encounters"][0])
    unselected = deepcopy(saved["photos"][1])
    current = photo(1, mask_path="current.png" if complete else None, flag="flagged", rating=4,
                    analysis_provenance={"detection": {"status": "complete", "current": True,
                                                       "no_qualifying_subject": not complete},
                                         "segmentation": {"status": "complete", "current": True} if complete else {}})
    current.update({field: .5 if complete else None for field in REQUIRED_QUALITY_FIELDS})
    seen = stub_features(monkeypatch, [current])
    monkeypatch.setattr(pipeline, "run_triage", lambda *a, **k: pytest.fail("Species refresh must not run quality triage"))
    refresh_selected_suggestions(object(), saved, [1], {})
    updated = saved["photos"][0]
    assert seen == [[1]]
    assert updated["mask_path"] == ("current.png" if complete else None)
    assert updated["subject_tenengrad"] == (.5 if complete else None)
    assert updated["analysis_status"] == "complete"
    assert updated["selection_eligible"] is complete
    assert updated["analysis_reasons"] == ([] if complete else ["no_qualifying_subject"])
    assert updated["label"] == "REVIEW" and updated["reject_reasons"] == []
    assert updated["flag"] == "flagged" and updated["rating"] == 4
    assert updated["review_note"] == "Keep annotation"
    assert "quality_composite" not in updated and "focus_score" not in updated
    assert saved["photos"][1] == unselected
    assert saved["encounters"][0]["photo_ids"] == original_group["photo_ids"]
    assert [b["photo_ids"] for b in saved["encounters"][0]["bursts"]] == [b["photo_ids"] for b in original_group["bursts"]]
    assert saved["encounters"][0]["bursts"][0]["species_override"] == original_group["bursts"][0]["species_override"]
    assert saved["encounters"][0]["species_confirmed"] is True
    assert saved["summary"]["review_count"] == 2


def test_species_refresh_persists_current_catalog_analysis_without_catalog_writes(db, tmp_path):
    from pipeline import load_results_raw, save_results_raw
    from services import photo_ingest

    folder = db.add_folder(str(tmp_path), name="Species review")
    pid = photo_ingest.add_photo(db, folder, "duck.jpg", ".jpg", 100, 1.0)
    db.model_runs.record_detector_run(pid, "megadetector-v6", 0)
    db.commit()
    saved = {"review_mode": "species", "photos": [photo(pid, mask_path="stale.png",
                analysis_status="not_run", selection_eligible=True)],
             "encounters": [group([pid], "Mandarin Duck")], "summary": {}}
    before = list(db.conn.iterdump())
    refresh_selected_suggestions(db, saved, [pid], {})
    save_results_raw(saved, str(tmp_path), db.require_workspace_id())
    reopened = load_results_raw(str(tmp_path), db.require_workspace_id())
    refreshed = reopened["photos"][0]
    assert refreshed["mask_path"] is None
    assert refreshed["analysis_status"] == "complete"
    assert refreshed["analysis_reasons"] == ["no_qualifying_subject"]
    assert refreshed["selection_eligible"] is False
    assert refreshed["label"] == "REVIEW"
    assert list(db.conn.iterdump()) == before
