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


def _write_species_conflict_pipeline_cache(live_server, photo_ids):
    db = live_server["db"]
    placeholders = ",".join("?" for _ in photo_ids)
    rows = db.conn.execute(
        f"SELECT id, filename, timestamp FROM photos WHERE id IN ({placeholders}) ORDER BY id",
        photo_ids,
    ).fetchall()
    photos = []
    predictions = [
        [("Mute Swan", 0.93, "Bird model"), ("Little Grebe", 0.04, "Bird model")],
        [("Little Grebe", 0.96, "Bird model"), ("Mute Swan", 0.04, "Bird model")],
        [("Little Grebe", 0.88, "Bird model"), ("Mute Swan", 0.09, "Bird model")],
    ]
    for row, species_top5 in zip(rows, predictions, strict=True):
        photos.append(
            {
                "id": row["id"],
                "filename": row["filename"],
                "timestamp": row["timestamp"],
                "label": "REVIEW",
                "quality_composite": 0.5,
                "flag": "none",
                "rating": 0,
                "species_top5": species_top5,
            }
        )

    ids = [photo["id"] for photo in photos]
    cache = {
        "photos": photos,
        "encounters": [
            {
                "photo_ids": ids,
                "photo_count": len(ids),
                "burst_count": 2,
                "time_range": [photos[0]["timestamp"], photos[-1]["timestamp"]],
                "species": ["Mute Swan", 0.35],
                "species_predictions": [],
                "species_confirmed": False,
                "confirmed_species": None,
                "bursts": [
                    {
                        "photo_ids": ids[:1],
                        "species_predictions": [],
                        "species_override": None,
                    },
                    {
                        "photo_ids": ids[1:],
                        "species_predictions": [],
                        "species_override": None,
                    },
                ],
            }
        ],
        "summary": {
            "total_photos": len(ids),
            "encounter_count": 1,
            "burst_count": 2,
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


def test_pipeline_review_search_matches_filename_without_species_predictions(
    live_server, page
):
    photo_ids = live_server["data"]["photos"][1:3]
    _write_predictionless_pipeline_cache(live_server, photo_ids)

    page.goto(f"{live_server['url']}/pipeline/review")

    search = page.locator("#speciesFilterInput")
    expect(search).to_have_attribute("placeholder", "Search species or filename...")
    expect(page.locator(".encounter-card")).to_have_count(1)

    search.fill("hawk2.jpg")
    expect(page.locator(".encounter-card")).to_have_count(1)
    expect(page.locator("#countAll")).to_have_text(" (2)")

    search.fill("not-a-photo.jpg")
    expect(page.locator(".encounter-card")).to_have_count(0)
    expect(page.locator("#countAll")).to_have_text(" (0)")


def _write_low_confidence_consensus_pipeline_cache(live_server, photo_ids):
    """Cache an encounter whose consensus label sits below the default slider.

    The classifier picks 'Mute Swan' as the encounter-level winner with an
    average confidence of 0.35 (below the default 40% slider). Every
    prediction that shares that species also stays under the threshold, so
    the encounter should NOT match a species search unless the user drops
    the slider below 35%.
    """
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
                "species": ["Mute Swan", 0.35],
                "species_predictions": [
                    {
                        "species": "Mute Swan",
                        "count": len(ids),
                        "avg_confidence": 0.35,
                        "models": [{"model": "Bird model", "confidence": 0.35}],
                    }
                ],
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


def test_pipeline_review_search_respects_min_confidence_for_consensus_label(
    live_server, page
):
    photo_ids = live_server["data"]["photos"][1:3]
    _write_low_confidence_consensus_pipeline_cache(live_server, photo_ids)

    page.goto(f"{live_server['url']}/pipeline/review")

    expect(page.locator(".encounter-card")).to_have_count(1)

    search = page.locator("#speciesFilterInput")
    search.fill("Mute Swan")

    # The consensus label is displayed on the card but its confidence (0.35)
    # is below the default 40% slider, so the search must not match through
    # the encounter's classifier-derived label.
    expect(page.locator(".encounter-card")).to_have_count(0)
    expect(page.locator("#countAll")).to_have_text(" (0)")

    # Dropping the slider under the prediction's confidence brings the
    # encounter back — the search still works, just gated by the threshold.
    page.evaluate("setMinConfidence(30)")
    expect(page.locator(".encounter-card")).to_have_count(1)
    expect(page.locator("#countAll")).to_have_text(" (2)")


def _write_unconfirmed_burst_override_pipeline_cache(live_server, photo_ids):
    """Cache an encounter whose burst carries an unconfirmed classifier-derived
    override (as `detach-photo` and group-review paths stamp them). The override
    species also has a burst-level prediction with confidence 0.35 — below the
    default 40% slider — so searching that species must NOT match through the
    unconfirmed override; only the threshold-gated prediction path applies.
    """
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
                        "species_predictions": [
                            {
                                "species": "Mute Swan",
                                "count": len(ids),
                                "avg_confidence": 0.35,
                                "models": [{"model": "Bird model", "confidence": 0.35}],
                            }
                        ],
                        "species_override": {"species": "Mute Swan", "confirmed": False},
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


def test_pipeline_review_search_gates_unconfirmed_burst_override(live_server, page):
    photo_ids = live_server["data"]["photos"][1:3]
    _write_unconfirmed_burst_override_pipeline_cache(live_server, photo_ids)

    page.goto(f"{live_server['url']}/pipeline/review")

    expect(page.locator(".encounter-card")).to_have_count(1)

    search = page.locator("#speciesFilterInput")
    search.fill("Mute Swan")

    # The burst override is unconfirmed (classifier-derived) and its backing
    # prediction sits at 0.35 — below the default 40% slider — so the search
    # must not surface it via the override bypass.
    expect(page.locator(".encounter-card")).to_have_count(0)
    expect(page.locator("#countAll")).to_have_text(" (0)")

    # Dropping the slider below the prediction's confidence brings the
    # encounter back through the gated burst prediction path.
    page.evaluate("setMinConfidence(30)")
    expect(page.locator(".encounter-card")).to_have_count(1)
    expect(page.locator("#countAll")).to_have_text(" (2)")


def _write_confirmed_burst_override_pipeline_cache(live_server, photo_ids):
    """Cache an encounter whose burst carries a *confirmed* override for a
    species with no supporting predictions. Confirmed overrides are manual
    labels, so they must bypass the confidence slider regardless of any
    classifier prediction (or lack thereof).
    """
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
                        "species_override": {"species": "Mute Swan", "confirmed": True},
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


def _write_mixed_confirmed_burst_pipeline_cache(live_server, hawk_id, robin_id):
    """Cache an encounter with two bursts: the first (confirmed) burst carries
    a confirmed override for 'Red-tailed Hawk' with filename hawk1.jpg; the
    second (unconfirmed) burst has no override and holds robin1.jpg. When Hide
    confirmed is on, the confirmed burst should not contribute its species or
    filename to the search — otherwise queries for the hidden burst's content
    would surface the unrelated visible burst.
    """
    db = live_server["db"]
    rows = db.conn.execute(
        "SELECT id, filename, timestamp FROM photos WHERE id IN (?, ?) ORDER BY id",
        (hawk_id, robin_id),
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
    hawk_photo = next(p for p in photos if p["id"] == hawk_id)
    robin_photo = next(p for p in photos if p["id"] == robin_id)
    cache = {
        "photos": photos,
        "encounters": [
            {
                "photo_ids": [hawk_photo["id"], robin_photo["id"]],
                "photo_count": 2,
                "burst_count": 2,
                "time_range": [hawk_photo["timestamp"], robin_photo["timestamp"]],
                "species": [],
                "species_predictions": [],
                "species_confirmed": False,
                "confirmed_species": None,
                "bursts": [
                    {
                        "photo_ids": [hawk_photo["id"]],
                        "species_predictions": [],
                        "species_override": {
                            "species": "Red-tailed Hawk",
                            "confirmed": True,
                        },
                    },
                    {
                        "photo_ids": [robin_photo["id"]],
                        "species_predictions": [
                            {
                                "species": "American Robin",
                                "count": 1,
                                "avg_confidence": 0.92,
                                "models": [
                                    {"model": "Bird model", "confidence": 0.92}
                                ],
                            }
                        ],
                        "species_override": None,
                    },
                ],
            }
        ],
        "summary": {
            "total_photos": 2,
            "encounter_count": 1,
            "burst_count": 2,
            "keep_count": 0,
            "review_count": 2,
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


def test_pipeline_review_search_scopes_to_visible_bursts_with_hide_confirmed(
    live_server, page
):
    photos = live_server["data"]["photos"]
    # hawk1.jpg (index 0) goes in the confirmed burst; robin1.jpg (index 3) is
    # the visible unconfirmed burst.
    _write_mixed_confirmed_burst_pipeline_cache(live_server, photos[0], photos[3])

    page.goto(f"{live_server['url']}/pipeline/review")
    expect(page.locator(".encounter-card")).to_have_count(1)
    expect(page.locator(".burst-strip")).to_have_count(2)

    # Baseline (Hide confirmed off): both burst species and filenames match.
    search = page.locator("#speciesFilterInput")
    search.fill("Red-tailed Hawk")
    expect(page.locator(".encounter-card")).to_have_count(1)
    search.fill("hawk1.jpg")
    expect(page.locator(".encounter-card")).to_have_count(1)

    search.fill("")
    page.locator("#hideConfirmedBtn").click()
    expect(page.locator(".burst-strip")).to_have_count(1)

    # With Hide confirmed on, searching the hidden burst's confirmed override
    # species must NOT match — otherwise the encounter would surface with only
    # its unrelated visible burst rendered.
    search.fill("Red-tailed Hawk")
    expect(page.locator(".encounter-card")).to_have_count(0)
    expect(page.locator("#countAll")).to_have_text(" (0)")

    # Same for the hidden burst's filename.
    search.fill("hawk1.jpg")
    expect(page.locator(".encounter-card")).to_have_count(0)
    expect(page.locator("#countAll")).to_have_text(" (0)")

    # The visible burst's species and filename still match.
    search.fill("American Robin")
    expect(page.locator(".encounter-card")).to_have_count(1)
    expect(page.locator("#countAll")).to_have_text(" (1)")

    search.fill("robin1.jpg")
    expect(page.locator(".encounter-card")).to_have_count(1)
    expect(page.locator("#countAll")).to_have_text(" (1)")

    # Toggling Hide confirmed back off restores matches on the confirmed
    # burst's species/filename.
    page.locator("#hideConfirmedBtn").click()
    search.fill("Red-tailed Hawk")
    expect(page.locator(".encounter-card")).to_have_count(1)
    expect(page.locator("#countAll")).to_have_text(" (2)")


def _write_aggregate_from_confirmed_burst_pipeline_cache(
    live_server, hawk_id, robin_id
):
    """Cache an encounter whose two bursts both contribute classifier
    predictions to the encounter-level aggregate. The confirmed burst
    predicts 'Red-tailed Hawk' at 0.92; the unconfirmed burst predicts
    'American Robin' at 0.92. The encounter aggregate carries both species,
    which is the shape the pipeline actually serializes (species_predictions
    is rolled up from every photo in the encounter). Under Hide confirmed,
    searching 'Red-tailed Hawk' must not surface the encounter — the only
    evidence for that species is in the hidden burst.
    """
    db = live_server["db"]
    rows = db.conn.execute(
        "SELECT id, filename, timestamp FROM photos WHERE id IN (?, ?) ORDER BY id",
        (hawk_id, robin_id),
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
    hawk_photo = next(p for p in photos if p["id"] == hawk_id)
    robin_photo = next(p for p in photos if p["id"] == robin_id)
    hawk_pred = {
        "species": "Red-tailed Hawk",
        "count": 1,
        "avg_confidence": 0.92,
        "models": [{"model": "Bird model", "confidence": 0.92}],
    }
    robin_pred = {
        "species": "American Robin",
        "count": 1,
        "avg_confidence": 0.92,
        "models": [{"model": "Bird model", "confidence": 0.92}],
    }
    cache = {
        "photos": photos,
        "encounters": [
            {
                "photo_ids": [hawk_photo["id"], robin_photo["id"]],
                "photo_count": 2,
                "burst_count": 2,
                "time_range": [hawk_photo["timestamp"], robin_photo["timestamp"]],
                "species": [],
                # Aggregate rolled up from every photo — includes the hawk
                # prediction contributed by the confirmed burst.
                "species_predictions": [hawk_pred, robin_pred],
                "species_confirmed": False,
                "confirmed_species": None,
                "bursts": [
                    {
                        "photo_ids": [hawk_photo["id"]],
                        "species_predictions": [hawk_pred],
                        "species_override": {
                            "species": "Red-tailed Hawk",
                            "confirmed": True,
                        },
                    },
                    {
                        "photo_ids": [robin_photo["id"]],
                        "species_predictions": [robin_pred],
                        "species_override": None,
                    },
                ],
            }
        ],
        "summary": {
            "total_photos": 2,
            "encounter_count": 1,
            "burst_count": 2,
            "keep_count": 0,
            "review_count": 2,
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


def test_pipeline_review_search_scopes_aggregate_predictions_to_visible_bursts(
    live_server, page
):
    photos = live_server["data"]["photos"]
    # hawk1.jpg goes in the confirmed burst; robin1.jpg is the visible burst.
    _write_aggregate_from_confirmed_burst_pipeline_cache(
        live_server, photos[0], photos[3]
    )

    page.goto(f"{live_server['url']}/pipeline/review")
    expect(page.locator(".encounter-card")).to_have_count(1)
    expect(page.locator(".burst-strip")).to_have_count(2)

    search = page.locator("#speciesFilterInput")

    # Baseline (Hide confirmed off): the aggregate carries both species, so
    # 'Red-tailed Hawk' matches via either the confirmed burst's override or
    # the encounter aggregate.
    search.fill("Red-tailed Hawk")
    expect(page.locator(".encounter-card")).to_have_count(1)

    search.fill("")
    page.locator("#hideConfirmedBtn").click()
    expect(page.locator(".burst-strip")).to_have_count(1)

    # With Hide confirmed on, the encounter aggregate still contains
    # 'Red-tailed Hawk' (rolled up from the hidden burst), but no visible
    # burst supports it — searching must not surface the encounter with only
    # the unrelated Robin burst rendered.
    search.fill("Red-tailed Hawk")
    expect(page.locator(".encounter-card")).to_have_count(0)
    expect(page.locator("#countAll")).to_have_text(" (0)")

    # The visible burst's species still matches.
    search.fill("American Robin")
    expect(page.locator(".encounter-card")).to_have_count(1)
    expect(page.locator("#countAll")).to_have_text(" (1)")


def _write_mixed_encounter_prior_label_pipeline_cache(
    live_server, hawk_id, robin_id
):
    """Cache a mixed encounter where the encounter-level confirmation was
    withdrawn (species_confirmed=False) but the serializer keeps the prior
    'Red-tailed Hawk' label in enc.confirmed_species for replacement flows.
    The only burst still carrying that label is a confirmed override on the
    hawk burst (which Hide confirmed will hide); the visible robin burst has
    no override. Searching the prior label under Hide confirmed must not
    surface the encounter.
    """
    db = live_server["db"]
    rows = db.conn.execute(
        "SELECT id, filename, timestamp FROM photos WHERE id IN (?, ?) ORDER BY id",
        (hawk_id, robin_id),
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
    hawk_photo = next(p for p in photos if p["id"] == hawk_id)
    robin_photo = next(p for p in photos if p["id"] == robin_id)
    cache = {
        "photos": photos,
        "encounters": [
            {
                "photo_ids": [hawk_photo["id"], robin_photo["id"]],
                "photo_count": 2,
                "burst_count": 2,
                "time_range": [hawk_photo["timestamp"], robin_photo["timestamp"]],
                "species": [],
                "species_predictions": [],
                # Mixed encounter: the encounter itself is unconfirmed, but a
                # prior label survives (serializer keeps it for replacement).
                "species_confirmed": False,
                "confirmed_species": "Red-tailed Hawk",
                "bursts": [
                    {
                        "photo_ids": [hawk_photo["id"]],
                        "species_predictions": [],
                        "species_override": {
                            "species": "Red-tailed Hawk",
                            "confirmed": True,
                        },
                    },
                    {
                        "photo_ids": [robin_photo["id"]],
                        "species_predictions": [
                            {
                                "species": "American Robin",
                                "count": 1,
                                "avg_confidence": 0.92,
                                "models": [
                                    {"model": "Bird model", "confidence": 0.92}
                                ],
                            }
                        ],
                        "species_override": None,
                    },
                ],
            }
        ],
        "summary": {
            "total_photos": 2,
            "encounter_count": 1,
            "burst_count": 2,
            "keep_count": 0,
            "review_count": 2,
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


def test_pipeline_review_search_scopes_mixed_encounter_prior_label(
    live_server, page
):
    photos = live_server["data"]["photos"]
    _write_mixed_encounter_prior_label_pipeline_cache(
        live_server, photos[0], photos[3]
    )

    page.goto(f"{live_server['url']}/pipeline/review")
    expect(page.locator(".encounter-card")).to_have_count(1)
    expect(page.locator(".burst-strip")).to_have_count(2)

    search = page.locator("#speciesFilterInput")

    # Baseline (Hide confirmed off): the prior encounter label is still
    # searchable because the confirmed burst carrying it is visible.
    search.fill("Red-tailed Hawk")
    expect(page.locator(".encounter-card")).to_have_count(1)

    search.fill("")
    page.locator("#hideConfirmedBtn").click()
    expect(page.locator(".burst-strip")).to_have_count(1)

    # With Hide confirmed on, only the robin burst renders. The encounter is
    # unconfirmed at the encounter level, so the surviving prior label is
    # backed exclusively by the now-hidden confirmed hawk override. Searching
    # 'Red-tailed Hawk' must not surface the encounter.
    search.fill("Red-tailed Hawk")
    expect(page.locator(".encounter-card")).to_have_count(0)
    expect(page.locator("#countAll")).to_have_text(" (0)")

    # The visible burst's species still matches.
    search.fill("American Robin")
    expect(page.locator(".encounter-card")).to_have_count(1)
    expect(page.locator("#countAll")).to_have_text(" (1)")


def _write_visible_burst_below_threshold_pipeline_cache(
    live_server, hawk_id, robin_id
):
    """Cache an encounter whose encounter-level aggregate carries 'Peregrine
    Falcon' at high confidence (rolled up from the hidden confirmed hawk
    burst), while the visible robin burst only has a below-threshold Falcon
    prediction. Under Hide confirmed at the default 40% slider, the aggregate
    passes the threshold gate but the visible burst's own Falcon evidence is
    below it — searching 'Peregrine Falcon' must not surface the encounter.
    """
    db = live_server["db"]
    rows = db.conn.execute(
        "SELECT id, filename, timestamp FROM photos WHERE id IN (?, ?) ORDER BY id",
        (hawk_id, robin_id),
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
    hawk_photo = next(p for p in photos if p["id"] == hawk_id)
    robin_photo = next(p for p in photos if p["id"] == robin_id)
    hidden_pred = {
        "species": "Peregrine Falcon",
        "count": 1,
        "avg_confidence": 0.9,
        "models": [{"model": "Bird model", "confidence": 0.9}],
    }
    visible_pred = {
        "species": "Peregrine Falcon",
        "count": 1,
        "avg_confidence": 0.1,
        "models": [{"model": "Bird model", "confidence": 0.1}],
    }
    # Aggregate carries a Falcon entry whose top model confidence passes
    # the default slider — but that evidence comes from the hidden burst.
    aggregate_pred = {
        "species": "Peregrine Falcon",
        "count": 2,
        "avg_confidence": 0.5,
        "models": [{"model": "Bird model", "confidence": 0.9}],
    }
    cache = {
        "photos": photos,
        "encounters": [
            {
                "photo_ids": [hawk_photo["id"], robin_photo["id"]],
                "photo_count": 2,
                "burst_count": 2,
                "time_range": [hawk_photo["timestamp"], robin_photo["timestamp"]],
                "species": [],
                "species_predictions": [aggregate_pred],
                "species_confirmed": False,
                "confirmed_species": None,
                "bursts": [
                    {
                        "photo_ids": [hawk_photo["id"]],
                        "species_predictions": [hidden_pred],
                        "species_override": {
                            "species": "Red-tailed Hawk",
                            "confirmed": True,
                        },
                    },
                    {
                        "photo_ids": [robin_photo["id"]],
                        "species_predictions": [visible_pred],
                        "species_override": None,
                    },
                ],
            }
        ],
        "summary": {
            "total_photos": 2,
            "encounter_count": 1,
            "burst_count": 2,
            "keep_count": 0,
            "review_count": 2,
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


def test_pipeline_review_search_gates_visible_burst_support_by_confidence(
    live_server, page
):
    photos = live_server["data"]["photos"]
    _write_visible_burst_below_threshold_pipeline_cache(
        live_server, photos[0], photos[3]
    )

    page.goto(f"{live_server['url']}/pipeline/review")
    expect(page.locator(".encounter-card")).to_have_count(1)
    expect(page.locator(".burst-strip")).to_have_count(2)

    search = page.locator("#speciesFilterInput")

    # Baseline (Hide confirmed off): the aggregate Falcon prediction passes
    # the slider and matches.
    search.fill("Peregrine Falcon")
    expect(page.locator(".encounter-card")).to_have_count(1)

    search.fill("")
    page.locator("#hideConfirmedBtn").click()
    expect(page.locator(".burst-strip")).to_have_count(1)

    # With Hide confirmed on, the aggregate still passes threshold, but the
    # only visible burst has Falcon at 0.1 — below the 40% default. Aggregate
    # search must require threshold-passing visible-burst support, so it
    # does not surface the encounter.
    search.fill("Peregrine Falcon")
    expect(page.locator(".encounter-card")).to_have_count(0)
    expect(page.locator("#countAll")).to_have_text(" (0)")

    # Dropping the slider so the visible burst's Falcon prediction meets the
    # threshold brings the encounter back — visible-burst support is now real.
    page.evaluate("setMinConfidence(5)")
    search.fill("Peregrine Falcon")
    expect(page.locator(".encounter-card")).to_have_count(1)
    expect(page.locator("#countAll")).to_have_text(" (1)")


def test_pipeline_review_search_matches_confirmed_burst_override(live_server, page):
    photo_ids = live_server["data"]["photos"][1:3]
    _write_confirmed_burst_override_pipeline_cache(live_server, photo_ids)

    page.goto(f"{live_server['url']}/pipeline/review")

    expect(page.locator(".encounter-card")).to_have_count(1)

    search = page.locator("#speciesFilterInput")
    search.fill("Mute Swan")

    # Confirmed manual overrides bypass the slider — the encounter must match
    # even at the default threshold with no classifier predictions.
    expect(page.locator(".encounter-card")).to_have_count(1)
    expect(page.locator("#countAll")).to_have_text(" (2)")


def _write_cross_field_pipeline_cache(live_server, photo_ids):
    """Cache an encounter whose eligible predictions are 'Mute Grouse' and
    'Trumpeter Swan'. A multi-token search for 'Mute Swan' shouldn't match:
    no single field contains both tokens, even though 'Mute' appears in one
    prediction and 'Swan' in another.
    """
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
                "species_predictions": [
                    {
                        "species": "Mute Grouse",
                        "count": len(ids),
                        "avg_confidence": 0.9,
                        "models": [{"model": "Bird model", "confidence": 0.9}],
                    },
                    {
                        "species": "Trumpeter Swan",
                        "count": len(ids),
                        "avg_confidence": 0.85,
                        "models": [{"model": "Bird model", "confidence": 0.85}],
                    },
                ],
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


def test_pipeline_review_search_rejects_cross_field_token_split(live_server, page):
    photo_ids = live_server["data"]["photos"][1:3]
    _write_cross_field_pipeline_cache(live_server, photo_ids)

    page.goto(f"{live_server['url']}/pipeline/review")

    expect(page.locator(".encounter-card")).to_have_count(1)

    search = page.locator("#speciesFilterInput")
    search.fill("Mute Swan")

    # Neither 'Mute Grouse' nor 'Trumpeter Swan' contains the full query, so
    # the encounter must not match — even though each token appears once
    # across the two prediction fields.
    expect(page.locator(".encounter-card")).to_have_count(0)
    expect(page.locator("#countAll")).to_have_text(" (0)")

    # Searching a single prediction verbatim still matches, proving the
    # threshold-gated prediction path is unaffected.
    search.fill("Trumpeter Swan")
    expect(page.locator(".encounter-card")).to_have_count(1)
    expect(page.locator("#countAll")).to_have_text(" (2)")


def test_pipeline_review_species_confirm_ignores_duplicate_inflight_clicks(live_server, page):
    photo_ids = live_server["data"]["photos"][:2]
    _write_confirmation_pipeline_cache(live_server, photo_ids)

    page.goto(f"{live_server['url']}/pipeline/review")

    page.evaluate(
        """
        () => {
          window.__speciesPosts = [];
          const originalSafeFetch = window.safeFetch;
          window.safeFetch = (url, opts, cfg) => {
            if (String(url).includes('/api/encounters/species')) {
              window.__speciesPosts.push(JSON.parse(opts.body));
              return new Promise(resolve => {
                window.__resolveSpeciesPost = () => resolve({
                  ok: true,
                  species: 'American Robin',
                  keyword_id: 123,
                  photo_count: 1,
                  skipped_photo_ids: [],
                  low_confidence_photo_ids: [],
                });
              });
            }
            return originalSafeFetch(url, opts, cfg);
          };
        }
        """
    )

    post_count = page.evaluate(
        """
        () => {
          const event = { stopPropagation() {} };
          window.__confirmPromises = [
            confirmSpecies(event, 1, null, null),
            confirmSpecies(event, 1, null, null),
          ];
          return window.__speciesPosts.length;
        }
        """
    )

    assert post_count == 1
    expect(
        page.locator('[data-species-widget="1"][data-enc="1"] .species-confirm-btn').first
    ).to_be_disabled()

    page.evaluate("() => window.__resolveSpeciesPost()")
    page.evaluate("() => Promise.all(window.__confirmPromises)")

    posts = page.evaluate("() => window.__speciesPosts")
    assert len(posts) == 1
    assert posts[0]["species"] == "American Robin"
    assert posts[0]["photo_ids"] == [photo_ids[1]]
    expect(
        page.locator('[data-species-widget="1"][data-enc="1"] .species-confirm-btn')
    ).to_have_count(0)


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
    expect(page.locator(".photo-card.review")).to_have_count(0)

    page.evaluate(
        "(photoId) => window.setFlagFor(photoId, 'flagged')",
        photo_ids[0],
    )
    expect(page.locator(".photo-label")).to_have_count(1)
    expect(page.locator(".photo-label")).to_have_text("KEEP")
    expect(page.locator(".photo-card.keep")).to_have_count(1)
    expect(page.locator(".photo-card.review")).to_have_count(0)

    show_labels = page.locator("#showPhotoLabelsChk")
    expect(show_labels).not_to_be_checked()
    show_labels.check()
    expect(page.locator(".photo-label")).to_have_count(2)
    expect(page.locator(".photo-card.review")).to_have_count(1)
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


def test_pipeline_review_marks_photo_and_burst_species_conflicts(live_server, page):
    photo_ids = live_server["data"]["photos"][:3]
    _write_species_conflict_pipeline_cache(live_server, photo_ids)

    page.goto(f"{live_server['url']}/pipeline/review")

    conflicts = page.locator("[data-species-conflict]")
    expect(conflicts).to_have_count(2)
    expect(conflicts.first).to_contain_text("Little Grebe 96%")
    expect(conflicts.first).to_have_attribute(
        "title",
        re.compile(r"Strong classification conflict: 1 classifier averages Little Grebe at 96%.*Mute Swan averages 4%"),
    )
    expect(page.locator(".burst-species-conflict")).to_contain_text(
        "2 suggest Little Grebe"
    )
    expect(page.locator(".encounter-species-conflict").first).to_contain_text(
        "2 species conflicts"
    )
    expect(page.locator("#countSpeciesConflict")).to_have_text(" (2)")

    threshold_examples = page.evaluate(
        """
        () => ({
          possible: analyzePhotoSpeciesConflict({
            species_top5: [
              ['Little Grebe', 0.62, 'Bird model'],
              ['Mute Swan', 0.31, 'Bird model'],
            ],
          }, 'Mute Swan').severity,
          matching: analyzePhotoSpeciesConflict({
            species_top5: [
              ['Mute Swan', 0.94, 'Bird model'],
              ['Little Grebe', 0.03, 'Bird model'],
            ],
          }, 'Mute Swan').severity,
        })
        """
    )
    assert threshold_examples == {"possible": "possible", "matching": None}

    page.locator('[data-filter="SPECIES_CONFLICT"]').click()
    expect(page.locator(".photo-card")).to_have_count(2)
    expect(page.locator(".photo-card").filter(has_text="Little Grebe")).to_have_count(2)

    conflicts.first.click()
    expect(page.locator(".inspect-species-conflict")).to_be_visible()
    expect(page.locator(".inspect-species-conflict")).to_contain_text(
        "This photo averages Little Grebe at 96%"
    )


def _write_confirmed_burst_conflict_pipeline_cache(live_server, photo_ids):
    """A single encounter with two bursts: the first (confirmed) burst holds
    the only conflicting frame; the second is clean and unconfirmed so the
    encounter stays visible when hide-confirmed is on."""
    db = live_server["db"]
    placeholders = ",".join("?" for _ in photo_ids)
    rows = db.conn.execute(
        f"SELECT id, filename, timestamp FROM photos WHERE id IN ({placeholders}) ORDER BY id",
        photo_ids,
    ).fetchall()
    predictions = [
        [("Little Grebe", 0.96, "Bird model"), ("Mute Swan", 0.04, "Bird model")],
        [("Mute Swan", 0.92, "Bird model"), ("Little Grebe", 0.05, "Bird model")],
        [("Mute Swan", 0.9, "Bird model"), ("Little Grebe", 0.06, "Bird model")],
    ]
    photos = []
    for row, species_top5 in zip(rows, predictions, strict=True):
        photos.append(
            {
                "id": row["id"],
                "filename": row["filename"],
                "timestamp": row["timestamp"],
                "label": "REVIEW",
                "quality_composite": 0.5,
                "flag": "none",
                "rating": 0,
                "species_top5": species_top5,
            }
        )
    ids = [photo["id"] for photo in photos]
    cache = {
        "photos": photos,
        "encounters": [
            {
                "photo_ids": ids,
                "photo_count": len(ids),
                "burst_count": 2,
                "time_range": [photos[0]["timestamp"], photos[-1]["timestamp"]],
                "species": ["Mute Swan", 0.35],
                "species_predictions": [],
                "species_confirmed": False,
                "confirmed_species": None,
                "bursts": [
                    {
                        "photo_ids": ids[:1],
                        "species_predictions": [],
                        "species_override": {"species": "Mute Swan", "confirmed": True},
                    },
                    {
                        "photo_ids": ids[1:],
                        "species_predictions": [],
                        "species_override": None,
                    },
                ],
            }
        ],
        "summary": {
            "total_photos": len(ids),
            "encounter_count": 1,
            "burst_count": 2,
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


def test_pipeline_review_encounter_conflict_summary_respects_hide_confirmed(
    live_server, page
):
    photo_ids = live_server["data"]["photos"][:3]
    _write_confirmed_burst_conflict_pipeline_cache(live_server, photo_ids)

    page.goto(f"{live_server['url']}/pipeline/review")

    # Baseline: the conflict in the confirmed burst is counted at the
    # encounter level and in the SPECIES_CONFLICT filter.
    expect(page.locator(".encounter-species-conflict").first).to_contain_text(
        "1 species conflict"
    )
    expect(page.locator("#countSpeciesConflict")).to_have_text(" (1)")

    # Turn on Hide confirmed. The confirmed burst (which carries the only
    # conflicting frame) is no longer rendered, so the encounter header/footer
    # badges must disappear too — they used to keep counting hidden photos.
    page.locator("#hideConfirmedBtn").click()
    expect(page.locator(".encounter-card")).to_have_count(1)
    expect(page.locator(".burst-strip")).to_have_count(1)
    expect(page.locator(".encounter-species-conflict")).to_have_count(0)
    expect(page.locator("#countSpeciesConflict")).to_have_text(" (0)")

    # Toggling it back off should bring the badge back.
    page.locator("#hideConfirmedBtn").click()
    expect(page.locator(".encounter-species-conflict").first).to_contain_text(
        "1 species conflict"
    )
    expect(page.locator("#countSpeciesConflict")).to_have_text(" (1)")


def test_pipeline_review_conflicting_burst_can_split_and_undo(live_server, page):
    photo_ids = live_server["data"]["photos"][:3]
    _write_species_conflict_pipeline_cache(live_server, photo_ids)

    page.goto(f"{live_server['url']}/pipeline/review")

    split = page.locator(".burst-conflict-split")
    expect(split).to_have_count(1)
    split.click()

    expect(page.locator(".encounter-card")).to_have_count(2)
    expect(page.locator("[data-species-conflict]")).to_have_count(0)
    expect(page.locator("#undoToast")).to_be_visible()
    expect(page.locator("#undoMsg")).to_have_text("Burst detached from encounter")

    page.locator("#undoToast button").click()
    expect(page.locator(".encounter-card")).to_have_count(1)
    expect(page.locator("[data-species-conflict]")).to_have_count(2)
