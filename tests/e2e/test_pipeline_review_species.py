import json
import os

from playwright.sync_api import expect


def _write_predictionless_pipeline_cache(live_server, photo_ids):
    db = live_server["db"]
    rows = db.conn.execute(
        "SELECT id, filename, timestamp FROM photos WHERE id IN (?, ?) ORDER BY id",
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
