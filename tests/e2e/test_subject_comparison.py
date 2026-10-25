"""Best Batch keeps selected birds and native focus evidence together."""
import re
from pathlib import Path

import numpy as np
from PIL import Image
from playwright.sync_api import expect
from services import detection_store, photo_ingest, workspace_folders


def test_species_comparison_selects_ambiguous_bird_and_analyzes_it(page, live_server, tmp_path, monkeypatch):
    db = live_server["db"]
    folder = workspace_folders.add_folder(db, str(tmp_path), name="Subject comparison")
    pids, dids = [], []
    for index in range(2):
        pixels = np.full((300, 600, 3), 120, np.uint8)
        checker = ((np.indices((240, 200)).sum(axis=0) // 4 % 2) * 2 - 1) * (30 + 40 * index)
        pixels[30:270, 30:230] = (120 + checker)[..., None]
        path = tmp_path / f"DSC_{index:04d}.png"
        Image.fromarray(pixels).save(path)
        pid = photo_ingest.add_photo(db, folder_id=folder, filename=path.name, extension=".png",
                                    file_size=path.stat().st_size, file_mtime=path.stat().st_mtime)
        ids = detection_store.write_detection_batch(db, pid, "megadetector-v6", [
            {"box": {"x": .05, "y": .1, "w": .34, "h": .8}, "confidence": .9, "category": "animal"},
            {"box": {"x": .6, "y": .1, "w": .34, "h": .8}, "confidence": .95, "category": "animal"},
        ])
        for i, did in enumerate(ids):
            species = "Vermilion Flycatcher" if index == 0 or i == 0 else "Sparrow"
            db.predictions.add(did, species, .95, "test", status="accepted")
        pids.append(pid)
        dids.append(ids)
    db.commit()

    def mask_for_box(image, box, variant):
        mask = np.zeros((image.height, image.width), bool)
        x, y, w, h = (box[k] for k in "xywh")
        mask[int(y * image.height):int((y + h) * image.height),
             int(x * image.width):int((x + w) * image.width)] = True
        return mask
    monkeypatch.setattr("masking.generate_mask", mask_for_box)
    page.goto(live_server["url"] + f"/best-batch?photo_id={pids[0]}")
    page.locator('#requestedSpecies').fill('Vermilion Flycatcher')
    page.locator('#compareSubjectsBtn').click()
    expect(page.locator('#rankList .rank-card')).to_have_count(2)
    expect(page.locator('#subjectEvidenceStatus')).to_contain_text('Several subjects match')
    page.locator('#subjectChoices').select_option(str(dids[0][0]))
    expect(page.locator('#subjectEvidenceStatus')).to_contain_text('Subject chosen for this comparison')
    page.locator('#analyzeSubjectsBtn').click()
    expect(page.locator('#bestSubtitle')).to_contain_text('2 ranked')
    expect(page.locator('#subjectEvidenceStatus')).to_contain_text('600 × 300 analysis image')
    expect(page.locator('#chosenSubjectImg')).to_have_attribute('src', f'/photos/{pids[0]}/crop?detection_id={dids[0][0]}')
    expect(page.locator('#focusGrid .native img').first).to_have_attribute('src', re.compile('detection_id=.*analysis_key='))
    expect(page.locator('#comparisonStatus')).to_contain_text('2 analyzed')
    expect(page.locator('#rankList')).not_to_contain_text('#null')
    page.locator('#compareSubjectsBtn').click()
    expect(page.locator('#bestSubtitle')).to_contain_text('2 ranked')
    expect(page.locator('#subjectChoices')).to_have_value(str(dids[0][0]))
    assert db.subjects.choice(pids[0]) is None
    page.screenshot(path=str(Path(__file__).resolve().parents[2] / '.context' / 'subject-comparison.png'), full_page=True)
    page.locator('#requestedSpecies').fill('Sparrow')
    expect(page.locator('#flagBtn')).to_be_disabled()
    expect(page.locator('#comparisonStatus')).to_contain_text('Species changed')
