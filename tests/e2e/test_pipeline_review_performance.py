"""Keep large process-review grids responsive while preserving review controls."""

import io

from PIL import Image
from playwright.sync_api import expect


def _open_grid(page, live_server, encounter_count=1124):
    predictions = [
        {"species": species, "models": [{"model": "Test classifier", "confidence": confidence}]}
        for species, confidence in [("Red-crowned Parrot", .9), ("Red-crowned Amazon", .05),
                                    ("Green Parrot", .03), ("Macaw", .01), ("Robin", .01)]
    ]
    photos = []
    encounters = []
    for index in range(encounter_count):
        ids = []
        for _ in range(12 if index < 116 else 11):
            pid = len(photos) + 1
            ids.append(pid)
            photos.append({
                "id": pid, "filename": f"bird-{pid}.NEF", "label": "REVIEW", "flag": "none",
                "quality_composite": .5,
                "species_top5": [[p["species"], p["models"][0]["confidence"], "Test classifier"] for p in predictions],
            })
        groups = [ids[:6], ids[6:]] if index < 511 else [ids]
        encounters.append({
            "photo_ids": ids, "photo_count": len(ids), "burst_count": len(groups),
            "species": ["Red-crowned Parrot"], "species_predictions": predictions,
            "bursts": [{"photo_ids": group, "species_predictions": predictions} for group in groups],
        })
    results = {
        "photos": photos, "encounters": encounters,
        "summary": {"total_photos": len(photos), "encounter_count": len(encounters), "burst_count": sum(e["burst_count"] for e in encounters),
                    "review_count": len(photos), "keep_count": 0, "reject_count": 0},
    }
    page.route("**/api/pipeline/page-init", lambda route: route.fulfill(json={
        "results": results, "workspace_overrides": {},
        "review_readiness": {"state": "ready", "total_photos": len(photos)},
    }))
    # Exclude disk/image service timings from the rendering measurement.
    thumbnail = io.BytesIO()
    Image.new("RGB", (160, 107), "green").save(thumbnail, "JPEG")
    page.route("**/thumbnails/*", lambda route: route.fulfill(body=thumbnail.getvalue(), content_type="image/jpeg"))
    page.goto(f"{live_server['url']}/pipeline/review", wait_until="domcontentloaded")
    expect(page.locator("#encountersContainer .photo-card")).to_have_count(len(photos), timeout=30000)


def test_large_review_grid_keeps_unchanged_photos_and_scroll_position(live_server, page):
    _open_grid(page, live_server)
    result = page.evaluate("""() => {
        const container = document.getElementById('encountersContainer');
        const strip = container.querySelector('.burst-strip');
        const image = strip.querySelector('.photo-card img');
        strip.scrollLeft = 200;
        const scrollLeft = strip.scrollLeft;
        const before = performance.now();
        pipelineResults.photos[pipelineResults.photos.length - 1].flag = 'rejected';
        renderResults();
        const height = container.offsetHeight;
        return {elapsed: performance.now() - before, height, nodes: container.querySelectorAll('*').length,
                sameImage: container.querySelector('.photo-card img') === image,
                scrollLeft: container.querySelector('.burst-strip').scrollLeft, originalScrollLeft: scrollLeft};
    }""")
    print("Large review grid:", result)
    assert result["sameImage"]
    assert result["originalScrollLeft"] > 0
    assert result["scrollLeft"] == result["originalScrollLeft"]
    expect(page.locator('.photo-card[data-photo-id="12480"] .flag-rejected')).to_have_text("X")

    # Sorting keeps canonical encounter indices, image nodes and burst actions.
    sorted_result = page.evaluate("""() => {
        const first = document.querySelector('.encounter-card[data-encounter-index="0"]');
        setEncounterSort('photos_asc');
        return {firstIndex: document.querySelector('.encounter-card').dataset.encounterIndex,
                sameCard: document.querySelector('.encounter-card[data-encounter-index="0"]') === first};
    }""")
    assert sorted_result == {"firstIndex": "116", "sameCard": True}
    expect(page.locator(".encounter-card")).to_have_count(1124)
    page.locator("#speciesFilterInput").fill("bird-12480.NEF")
    expect(page.locator(".encounter-card")).to_have_count(1)
    expect(page.locator(".encounter-card")).to_have_attribute("data-encounter-index", "1123")
    assert page.evaluate("groupFlagTarget(1123, 0).photoIds") == list(range(12470, 12481))
    page.locator("#speciesFilterInput").fill("")
    expect(page.locator(".encounter-card")).to_have_count(1124)


def test_review_grid_species_menu_opens_offscreen_and_confirms(live_server, page):
    _open_grid(page, live_server, encounter_count=3)
    writes = []

    def confirm(route):
        body = route.request.post_data_json
        if body.get('preview'):
            route.fulfill(json={'ok': True, 'preview_token': 'current-photos', 'photo_count': len(body['photo_ids']),
                'photos': [{'id': pid, 'filename': f'bird-{pid}.NEF', 'selected': True,
                            'subjects': [], 'existing_species': [], 'additions': [body['species']], 'removals': []}
                           for pid in body['photo_ids']]})
            return
        writes.append(body)
        route.fulfill(json={"ok": True})

    page.route("**/api/encounters/species", confirm)
    # Closed menus should not create thousands of unused search inputs/rows.
    expect(page.locator(".species-dropdown input")).to_have_count(0)
    last = page.locator('.encounter-card[data-encounter-index="2"]')
    last.scroll_into_view_if_needed()
    last.locator(".species-name").first.click()
    menu = page.locator(".species-dropdown.open")
    expect(menu.locator("input")).to_be_visible()
    expect(menu.locator(".species-dropdown-item")).to_have_count(5)
    menu.get_by_text("Red-crowned Amazon", exact=True).click()
    expect(page.locator('#speciesConfirmationPreview .species-preview-apply')).to_be_enabled()
    page.locator('#speciesConfirmationPreview .species-preview-apply').click()
    expect(last.locator(".species-name").first).to_have_text("Red-crowned Amazon")
    expect(last.locator(".species-name").first).to_have_class("species-name confirmed")
    assert writes == [{"species": "Red-crowned Amazon", "photo_ids": list(range(25, 37)),
                       "source_photo_ids": list(range(25, 37)), "add": True,
                       "split": False, "preview_token": "current-photos"}]

    # A full render after partial widget updates must retain the confirmed
    # state, and Hide confirmed must remove exactly that encounter.
    page.evaluate("renderResults()")
    expect(last.locator(".species-name").first).to_have_class("species-name confirmed")
    page.locator("#hideConfirmedBtn").click()
    expect(page.locator(".encounter-card")).to_have_count(2)
    expect(page.locator(".photo-card")).to_have_count(24)


def test_review_grid_conflict_evidence_refreshes_after_in_place_edits(live_server, page):
    _open_grid(page, live_server, encounter_count=1)
    page.evaluate("""() => {
        pipelineResults.photos[0].species_top5[0][1] = .05;
        pipelineResults.photos[0].species_top5[1][1] = .95;
        renderResults();
    }""")
    conflict = page.locator('.photo-card[data-photo-id="1"] .photo-species-conflict')
    expect(conflict).to_have_attribute("data-species-conflict", "strong")
    expect(conflict).to_contain_text("Red-crowned Amazon")
    page.evaluate("""() => {
        pipelineResults.encounters[0].bursts[0].species_override = {
            species: 'Red-crowned Amazon', confirmed: true
        };
        renderResults();
    }""")
    expect(conflict).to_have_count(0)


def test_review_grid_reload_resets_collapsed_encounters(live_server, page):
    _open_grid(page, live_server, encounter_count=2)
    page.locator("#encChev0").click()
    expect(page.locator("#encBody0")).to_be_hidden()
    page.evaluate("applyReviewResults(cloneReviewData(pipelineResults), resultsCacheInfo)")
    expect(page.locator("#encBody0")).to_be_visible()
    expect(page.locator("#encChev0")).not_to_have_class("encounter-chevron collapsed")
    assert page.evaluate("collapsedEncounters.size") == 0
    page.evaluate("renderResults()")
    expect(page.locator("#encBody0")).to_be_visible()


def test_review_grid_releases_closed_species_menus(live_server, page):
    _open_grid(page, live_server, encounter_count=3)
    for index in range(3):
        card = page.locator(f'.encounter-card[data-encounter-index="{index}"]')
        card.scroll_into_view_if_needed()
        card.locator(".species-name").first.click()
        expect(page.locator(".species-dropdown input")).to_have_count(1)
        expect(page.locator(".species-dropdown-item")).to_have_count(5)
    page.locator(".encounter-header").last.click()
    expect(page.locator(".species-dropdown input")).to_have_count(0)
    expect(page.locator(".species-dropdown-item")).to_have_count(0)

    # Reused cards must not retain closed controls after a full render, and
    # opening the same menu again must restore its predictions.
    page.evaluate("renderResults()")
    expect(page.locator(".species-dropdown input")).to_have_count(0)
    card.locator(".species-name").first.click()
    expect(page.locator(".species-dropdown.open input")).to_be_visible()
    expect(page.locator(".species-dropdown-item")).to_have_count(5)
