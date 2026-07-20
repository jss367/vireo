"""Process-review controls for rejecting a burst or full encounter."""

import json
import os

from playwright.sync_api import expect


def _write_grouped_pipeline_cache(live_server, photo_ids):
    db = live_server["db"]
    placeholders = ",".join("?" for _ in photo_ids)
    rows = db.conn.execute(
        f"SELECT id, filename, timestamp, flag FROM photos "
        f"WHERE id IN ({placeholders}) ORDER BY id",
        photo_ids,
    ).fetchall()
    photos = [
        {
            "id": row["id"],
            "filename": row["filename"],
            "timestamp": row["timestamp"],
            "label": "REVIEW",
            "quality_composite": 0.5,
            "flag": row["flag"],
            "rating": 0,
        }
        for row in rows
    ]
    ids = [photo["id"] for photo in photos]
    bursts = [
        {"photo_ids": ids[:2], "species_predictions": [], "species_override": None},
        {"photo_ids": ids[2:], "species_predictions": [], "species_override": None},
    ]
    cache = {
        "photos": photos,
        "encounters": [
            {
                "photo_ids": ids,
                "photo_count": len(ids),
                "burst_count": len(bursts),
                "time_range": [photos[0]["timestamp"], photos[-1]["timestamp"]],
                "species": [],
                "species_predictions": [],
                "species_confirmed": False,
                "confirmed_species": None,
                "bursts": bursts,
            }
        ],
        "summary": {
            "total_photos": len(ids),
            "encounter_count": 1,
            "burst_count": len(bursts),
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
    with open(path, "w") as cache_file:
        json.dump(cache, cache_file)


def _flags(db, photo_ids):
    placeholders = ",".join("?" for _ in photo_ids)
    rows = db.conn.execute(
        f"SELECT id, flag FROM photos WHERE id IN ({placeholders}) ORDER BY id",
        photo_ids,
    ).fetchall()
    return [row["flag"] for row in rows]


def test_reject_burst_and_undo_restores_prior_flags(live_server, page):
    db = live_server["db"]
    photo_ids = live_server["data"]["photos"][:4]
    db.update_photo_flag(photo_ids[0], "flagged")
    _write_grouped_pipeline_cache(live_server, photo_ids)

    page.goto(f"{live_server['url']}/pipeline/review")
    burst_buttons = page.get_by_test_id("reject-burst")
    expect(burst_buttons).to_have_count(2)

    burst_buttons.first.click()

    expect(page.locator("#undoMsg")).to_have_text("Rejected 2 photos in burst")
    expect(page.get_by_test_id("reject-burst").first).to_have_attribute(
        "aria-label", "Clear rejects"
    )
    assert _flags(db, photo_ids) == ["rejected", "rejected", "none", "none"]
    expect(
        page.locator(f'.photo-card[data-photo-id="{photo_ids[0]}"] .flag-rejected')
    ).to_have_text("X")

    page.locator("#undoToast .undo-toast-btn").click()

    expect(page.get_by_test_id("reject-burst").first).to_have_attribute(
        "aria-label", "Reject burst"
    )
    expect(page.get_by_text("Restored previous flags for burst", exact=True)).to_be_visible()
    assert _flags(db, photo_ids) == ["flagged", "none", "none", "none"]


def test_reject_and_clear_full_encounter(live_server, page):
    db = live_server["db"]
    photo_ids = live_server["data"]["photos"][:4]
    _write_grouped_pipeline_cache(live_server, photo_ids)

    page.goto(f"{live_server['url']}/pipeline/review")
    encounter_button = page.get_by_test_id("reject-encounter")
    expect(encounter_button).to_have_attribute("aria-label", "Reject encounter")

    encounter_button.click()

    expect(encounter_button).to_have_attribute("aria-label", "Clear rejects")
    expect(page.locator("#undoMsg")).to_have_text("Rejected 4 photos in encounter")
    assert _flags(db, photo_ids) == ["rejected"] * 4

    encounter_button.click()

    expect(encounter_button).to_have_attribute("aria-label", "Reject encounter")
    expect(page.locator("#undoMsg")).to_have_text(
        "Cleared rejects from 4 photos in encounter"
    )
    assert _flags(db, photo_ids) == ["none"] * 4


def _write_partially_confirmed_pipeline_cache(live_server, photo_ids):
    """Encounter with a confirmed first burst and an unconfirmed second burst."""
    db = live_server["db"]
    placeholders = ",".join("?" for _ in photo_ids)
    rows = db.conn.execute(
        f"SELECT id, filename, timestamp, flag FROM photos "
        f"WHERE id IN ({placeholders}) ORDER BY id",
        photo_ids,
    ).fetchall()
    photos = [
        {
            "id": row["id"],
            "filename": row["filename"],
            "timestamp": row["timestamp"],
            "label": "REVIEW",
            "quality_composite": 0.5,
            "flag": row["flag"],
            "rating": 0,
        }
        for row in rows
    ]
    ids = [photo["id"] for photo in photos]
    bursts = [
        {
            "photo_ids": ids[:2],
            "species_predictions": [],
            "species_override": {"species": "American Robin", "confirmed": True},
        },
        {"photo_ids": ids[2:], "species_predictions": [], "species_override": None},
    ]
    cache = {
        "photos": photos,
        "encounters": [
            {
                "photo_ids": ids,
                "photo_count": len(ids),
                "burst_count": len(bursts),
                "time_range": [photos[0]["timestamp"], photos[-1]["timestamp"]],
                "species": [],
                "species_predictions": [],
                "species_confirmed": False,
                "confirmed_species": None,
                "bursts": bursts,
            }
        ],
        "summary": {
            "total_photos": len(ids),
            "encounter_count": 1,
            "burst_count": len(bursts),
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
    with open(path, "w") as cache_file:
        json.dump(cache, cache_file)


def test_encounter_reject_skips_hidden_confirmed_bursts(live_server, page):
    """With `Hide confirmed` active, the encounter-level Reject/Clear button
    must only touch the bursts that are actually rendered — not the ones the
    user hid by confirming their species. Regression for the case where the
    header control read `Clear rejects`/`Reject encounter` off the whole
    photo list, so clicking it could flip flags on invisible photos."""
    db = live_server["db"]
    photo_ids = live_server["data"]["photos"][:4]
    _write_partially_confirmed_pipeline_cache(live_server, photo_ids)

    page.goto(f"{live_server['url']}/pipeline/review")
    expect(page.locator(".burst-strip")).to_have_count(2)

    page.locator("#hideConfirmedBtn").click()
    expect(page.locator(".burst-strip")).to_have_count(1)

    encounter_button = page.get_by_test_id("reject-encounter")
    expect(encounter_button).to_have_attribute("aria-label", "Reject encounter")

    encounter_button.click()

    expect(encounter_button).to_have_attribute("aria-label", "Clear rejects")
    expect(page.locator("#undoMsg")).to_have_text("Rejected 2 photos in encounter")
    # First (confirmed, hidden) burst must be untouched; only the visible
    # unconfirmed burst's photos are rejected.
    assert _flags(db, photo_ids) == ["none", "none", "rejected", "rejected"]

    encounter_button.click()

    expect(encounter_button).to_have_attribute("aria-label", "Reject encounter")
    expect(page.locator("#undoMsg")).to_have_text(
        "Cleared rejects from 2 photos in encounter"
    )
    assert _flags(db, photo_ids) == ["none"] * 4
