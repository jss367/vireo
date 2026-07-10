import json
import os
import re

from playwright.sync_api import expect


def _write_predictionless_pipeline_cache(live_server, photo_ids):
    db = live_server["db"]
    placeholders = ",".join("?" for _ in photo_ids)
    rows = db.conn.execute(
        f"SELECT id, filename, timestamp FROM photos WHERE id IN ({placeholders}) ORDER BY id",
        photo_ids,
    ).fetchall()
    photos = [
        {
            "id": row["id"],
            "filename": row["filename"],
            "timestamp": row["timestamp"],
            "label": "REVIEW",
            "quality_composite": 0.5,
            "flag": "none",
            "rating": 0,
        }
        for row in rows
    ]
    ids = [p["id"] for p in photos]
    cache = {
        "photos": photos,
        "encounters": [
            {
                "photo_ids": ids,
                "photo_count": len(ids),
                "burst_count": 1,
                "time_range": [photos[0]["timestamp"], photos[-1]["timestamp"]],
                "species": [],
                "species_predictions": [],
                "species_confirmed": False,
                "confirmed_species": None,
                "bursts": [
                    {
                        "photo_ids": ids,
                        "species_predictions": [],
                        "species_override": None,
                    }
                ],
            }
        ],
        "summary": {
            "total_photos": len(ids),
            "encounter_count": 1,
            "burst_count": 1,
            "keep_count": 0,
            "review_count": len(ids),
            "reject_count": 0,
            "rarity_protected": 0,
        },
    }
    path = os.path.join(
        os.path.dirname(db._db_path),
        f"pipeline_results_ws{db._active_workspace_id}.json",
    )
    with open(path, "w") as f:
        json.dump(cache, f)


def _write_confirmation_pipeline_cache(live_server, photo_ids):
    db = live_server["db"]
    placeholders = ",".join("?" for _ in photo_ids)
    rows = db.conn.execute(
        f"SELECT id, filename, timestamp FROM photos WHERE id IN ({placeholders}) ORDER BY id",
        photo_ids,
    ).fetchall()
    species = ["Red-tailed Hawk", "American Robin"]
    photos = []
    photo_species = []
    for idx, row in enumerate(rows):
        species_name = species[idx % len(species)]
        photo_species.append(species_name)
        photos.append({
            "id": row["id"],
            "filename": row["filename"],
            "timestamp": row["timestamp"],
            "label": "REVIEW",
            "quality_composite": 0.5,
            "flag": "none",
            "rating": 0,
            "confirmed_species": species_name if idx == 0 else None,
        })
    encounters = []
    for idx, photo in enumerate(photos):
        species_name = photo_species[idx]
        confirmed = idx == 0
        encounters.append(
            {
                "photo_ids": [photo["id"]],
                "photo_count": 1,
                "burst_count": 1,
                "time_range": [photo["timestamp"], photo["timestamp"]],
                "species": [species_name],
                "species_predictions": [
                    {"species": species_name, "models": [{"confidence": 0.92}]},
                ],
                "species_confirmed": confirmed,
                "confirmed_species": species_name if confirmed else None,
                "bursts": [
                    {
                        "photo_ids": [photo["id"]],
                        "species_predictions": [
                            {"species": species_name, "models": [{"confidence": 0.92}]},
                        ],
                        "species_override": (
                            {"species": species_name, "confirmed": True}
                            if confirmed
                            else None
                        ),
                    }
                ],
            }
        )
    cache = {
        "photos": photos,
        "encounters": encounters,
        "summary": {
            "total_photos": len(photos),
            "encounter_count": len(encounters),
            "burst_count": len(encounters),
            "keep_count": 0,
            "review_count": len(photos),
            "reject_count": 0,
            "rarity_protected": 0,
            "confirmed_count": 1,
            "unconfirmed_count": len(photos) - 1,
        },
    }
    path = os.path.join(
        os.path.dirname(db._db_path),
        f"pipeline_results_ws{db._active_workspace_id}.json",
    )
    with open(path, "w") as f:
        json.dump(cache, f)


def _review_result_for_ids(live_server, photo_ids):
    db = live_server["db"]
    placeholders = ",".join("?" for _ in photo_ids)
    rows = db.conn.execute(
        f"SELECT id, filename, timestamp FROM photos WHERE id IN ({placeholders}) ORDER BY id",
        photo_ids,
    ).fetchall()
    photos = [
        {
            "id": row["id"],
            "filename": row["filename"],
            "timestamp": row["timestamp"],
            "label": "REVIEW",
            "quality_composite": 0.5,
            "flag": "none",
            "rating": 0,
        }
        for row in rows
    ]
    ids = [p["id"] for p in photos]
    return {
        "photos": photos,
        "encounters": [
            {
                "photo_ids": ids,
                "photo_count": len(ids),
                "burst_count": 1 if ids else 0,
                "time_range": [
                    photos[0]["timestamp"] if photos else None,
                    photos[-1]["timestamp"] if photos else None,
                ],
                "species": [],
                "species_predictions": [],
                "species_confirmed": False,
                "confirmed_species": None,
                "bursts": [{"photo_ids": ids, "species_predictions": []}]
                if ids else [],
            }
        ] if ids else [],
        "summary": {
            "total_photos": len(ids),
            "encounter_count": 1 if ids else 0,
            "burst_count": 1 if ids else 0,
            "keep_count": 0,
            "review_count": len(ids),
            "reject_count": 0,
            "rarity_protected": 0,
        },
    }


def test_predictionless_pipeline_encounter_can_add_species(live_server, page):
    photo_ids = live_server["data"]["photos"][1:3]
    _write_predictionless_pipeline_cache(live_server, photo_ids)

    page.goto(f"{live_server['url']}/pipeline/review")

    species_name = page.locator(".species-widget .species-name").first
    expect(species_name).to_have_text("Add species")
    species_name.click()

    input_box = page.locator(".species-dropdown.open input").first
    input_box.fill("Yellow-breasted Chat")
    input_box.press("Enter")

    expect(species_name).to_have_text("Yellow-breasted Chat")

    db = live_server["db"]
    rows = db.conn.execute(
        """
        SELECT p.id
        FROM photos p
        JOIN photo_keywords pk ON pk.photo_id = p.id
        JOIN keywords k ON k.id = pk.keyword_id
        WHERE p.id IN (?, ?) AND k.name = ? AND k.is_species = 1
        """,
        (*photo_ids, "Yellow-breasted Chat"),
    ).fetchall()
    assert {r["id"] for r in rows} == set(photo_ids)


def test_species_name_arg_keeps_nullish_values_empty(live_server, page):
    page.goto(f"{live_server['url']}/pipeline/review")

    assert page.evaluate("speciesNameArg(null)") == "''"
    assert page.evaluate("speciesNameArg(undefined)") == "''"


def test_pipeline_review_explains_partial_cache(live_server, page):
    photo_ids = live_server["data"]["photos"][1:3]
    _write_predictionless_pipeline_cache(live_server, photo_ids)

    page.goto(f"{live_server['url']}/pipeline/review")

    banner = page.locator("[data-testid='pipeline-review-cache-scope']")
    expect(banner).to_be_visible()
    expect(banner).to_contain_text(
        f"Showing {len(photo_ids)} of {len(live_server['data']['photos'])}"
    )
    expect(banner).to_contain_text("selected folders")


def test_pipeline_review_scope_control_loads_workspace_and_collection(live_server, page):
    all_ids = live_server["data"]["photos"]
    cached_ids = all_ids[1:3]
    collection_ids = [all_ids[0]]
    _write_predictionless_pipeline_cache(live_server, cached_ids)

    payloads = []
    flag_payloads = []

    page.route(
        "**/api/collections",
        lambda route: route.fulfill(
            json=[{"id": 123, "name": "Selected Birds", "photo_count": 1}]
        ),
    )

    def regroup_live(route):
        body = route.request.post_data_json
        payloads.append(body)
        ids = collection_ids if body.get("collection_id") == 123 else all_ids
        route.fulfill(json=_review_result_for_ids(live_server, ids))

    page.route("**/api/pipeline/regroup-live", regroup_live)
    page.route(
        "**/api/photos/*/flag",
        lambda route: (
            flag_payloads.append(route.request.post_data_json),
            route.fulfill(json={"ok": True}),
        ),
    )

    page.goto(f"{live_server['url']}/pipeline/review")

    page.locator("[data-testid='pipeline-review-scope']").select_option("workspace")
    expect(page.locator("#statTotalPhotos")).to_have_text(str(len(all_ids)))
    assert payloads[-1]["save_cache"] is False
    assert "collection_id" not in payloads[-1]

    page.locator("[data-testid='pipeline-review-scope']").select_option("collection")
    page.locator("[data-testid='pipeline-review-collection']").select_option("123")
    expect(page.locator("#statTotalPhotos")).to_have_text("1")
    assert payloads[-1]["collection_id"] == 123
    assert payloads[-1]["save_cache"] is False

    page.evaluate(
        "(photoId) => window.setFlagFor(photoId, 'flagged')",
        collection_ids[0],
    )
    page.wait_for_timeout(100)
    assert flag_payloads == []


def test_pipeline_review_sidebar_collapses_and_persists(live_server, page):
    photo_ids = live_server["data"]["photos"][1:3]
    _write_predictionless_pipeline_cache(live_server, photo_ids)

    page.goto(f"{live_server['url']}/pipeline/review")

    layout = page.locator("#pipelineLayout")
    sidebar = page.locator("#pipelineSidebar")
    content = page.locator("#pipelineSidebarContent")
    toggle = page.locator("[data-testid='pipeline-sidebar-toggle']")

    expect(content).to_be_visible()
    expanded_width = sidebar.bounding_box()["width"]

    toggle.click()

    expect(layout).to_have_class(re.compile(r"\bsidebar-collapsed\b"))
    expect(content).to_be_hidden()
    expect(toggle).to_have_attribute("aria-expanded", "false")
    page.wait_for_timeout(250)
    collapsed_width = sidebar.bounding_box()["width"]
    assert collapsed_width < expanded_width
    assert collapsed_width <= 60

    page.reload()
    expect(layout).to_have_class(re.compile(r"\bsidebar-collapsed\b"))
    expect(content).to_be_hidden()
    expect(toggle).to_have_attribute("aria-expanded", "false")

    toggle.click()
    expect(layout).not_to_have_class(re.compile(r"\bsidebar-collapsed\b"))
    expect(content).to_be_visible()
    expect(toggle).to_have_attribute("aria-expanded", "true")


def test_pipeline_review_view_settings_persist(live_server, page):
    photo_ids = live_server["data"]["photos"][1:3]
    _write_confirmation_pipeline_cache(live_server, photo_ids)

    page.goto(f"{live_server['url']}/pipeline/review")
    expect(page.locator(".encounter-card")).to_have_count(2)
    expect(page.locator(".photo-label")).to_have_count(0)

    page.evaluate(
        "(photoId) => window.setFlagFor(photoId, 'flagged')",
        photo_ids[0],
    )
    expect(page.locator(".photo-label")).to_have_count(1)
    expect(page.locator(".photo-label")).to_have_text("KEEP")

    show_labels = page.locator("#showPhotoLabelsChk")
    expect(show_labels).not_to_be_checked()
    show_labels.check()
    expect(page.locator(".photo-label")).to_have_count(2)
    page.locator("#hideConfirmedBtn").click()
    page.locator('[data-filter="REVIEW"]').click()
    page.locator("#speciesFilterInput").fill("Robin")
    page.locator("#thumbSizeSlider").evaluate(
        """el => {
            el.value = '220';
            el.dispatchEvent(new Event('input', { bubbles: true }));
        }"""
    )

    expect(page.locator(".encounter-card")).to_have_count(1)
    page.reload()

    expect(page.locator("#hideConfirmedBtn")).to_have_class(re.compile(r"\bactive\b"))
    expect(page.locator('[data-filter="REVIEW"]')).to_have_class(re.compile(r"\bactive\b"))
    expect(page.locator("#speciesFilterInput")).to_have_value("Robin")
    expect(page.locator("#thumbSizeSlider")).to_have_value("220")
    expect(page.locator("#showPhotoLabelsChk")).to_be_checked()
    expect(page.locator(".photo-label")).to_have_count(1)
    expect(page.locator(".encounter-card")).to_have_count(1)

    page.reload()
    expect(page.locator("#speciesFilterInput")).to_have_value("Robin")
    expect(page.locator(".encounter-card")).to_have_count(1)

    page.locator("#speciesFilterClear").click()
    expect(page.locator("#speciesFilterInput")).to_have_value("")
    expect(page.locator(".encounter-card")).to_have_count(1)
