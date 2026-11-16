"""The lightbox says when a subject is a region the detector labeled person or vehicle."""
from PIL import Image
from playwright.sync_api import expect
from services import detection_store, photo_ingest, workspace_folders


def test_candidate_origin_in_subjects_and_inspector(live_server, page, tmp_path):
    db = live_server["db"]
    path = tmp_path / "candidate.jpg"
    Image.new("RGB", (640, 400), (80, 110, 90)).save(path)
    folder = workspace_folders.add_folder(db, str(tmp_path))
    photo = photo_ingest.add_photo(
        db, folder, path.name, extension=".jpg", file_size=path.stat().st_size,
        file_mtime=path.stat().st_mtime, width=640, height=400,
    )
    (region,) = detection_store.write_detection_batch(db, photo, "megadetector-v6", [
        {"box": {"x": .1, "y": .2, "w": .5, "h": .6},
         "confidence": .78, "category": "person"},
    ])
    db.candidate_subjects.record(region, {
        "detector_category": "person", "classifier_model": "BioCLIP",
        "labels_fingerprint": "validation", "top_species": "Wood Duck",
        "max_match_score": .55, "background_score": .19,
        "background_prompt": "water",
    }, promoted=True)
    db.predictions.add(region, "Wood Duck", .9, "BioCLIP")
    db.model_runs.record_classifier_match_score(
        region, "BioCLIP", "validation", .55, score_kind="cosine",
    )
    db.commit()
    page.goto(live_server["url"] + "/browse")
    page.evaluate("pid => openLightbox(pid, 'candidate.jpg')", photo)
    page.locator("#lightboxSubjectSummary").click()
    expect(page.locator("#lightboxSubjectDetails")).to_contain_text(
        "Detector region labeled person, kept because the classifier recognized an animal",
    )
    expect(page.locator("#lightboxSubjectDetails")).to_contain_text("Wood Duck")
    page.locator("#lightboxImg").click(button="right")
    page.locator(".vireo-ctx-item", has_text="Inspect Pipeline").click()
    expect(page.locator("#pipelineOverlay")).to_be_visible()
    expect(page.locator("#pipelineContent")).to_contain_text(
        "megadetector-v6 78% as person, kept: the classifier recognized an animal",
    )
