"""Process-review controls for rejecting a burst or full encounter."""

import json
import os
import time

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


def _write_mixed_label_pipeline_cache(live_server, photo_ids):
    """Encounter with a single burst whose photos carry mixed labels so
    changing the label filter hides some frames inside the burst."""
    db = live_server["db"]
    placeholders = ",".join("?" for _ in photo_ids)
    rows = db.conn.execute(
        f"SELECT id, filename, timestamp, flag FROM photos "
        f"WHERE id IN ({placeholders}) ORDER BY id",
        photo_ids,
    ).fetchall()
    # First two photos are KEEP, last two are REVIEW. Selecting the REVIEW
    # filter should hide the KEEP frames but still render the burst.
    labels = ["KEEP", "KEEP", "REVIEW", "REVIEW"]
    photos = [
        {
            "id": row["id"],
            "filename": row["filename"],
            "timestamp": row["timestamp"],
            "label": labels[idx],
            "quality_composite": 0.5,
            "flag": row["flag"],
            "rating": 0,
        }
        for idx, row in enumerate(rows)
    ]
    ids = [photo["id"] for photo in photos]
    bursts = [
        {"photo_ids": ids, "species_predictions": [], "species_override": None},
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
            "keep_count": 2,
            "review_count": 2,
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


def test_burst_reject_respects_active_label_filter(live_server, page):
    """When the Review label filter is active, clicking `Reject burst` must
    only touch photos that pass the filter. Regression for the case where the
    button targeted the raw burst photo list, so it could flip flags on
    KEEP/non-conflict frames hidden by the filter."""
    db = live_server["db"]
    photo_ids = live_server["data"]["photos"][:4]
    _write_mixed_label_pipeline_cache(live_server, photo_ids)

    page.goto(f"{live_server['url']}/pipeline/review")
    page.locator('.filter-btn[data-filter="REVIEW"]').click()
    # Two KEEP frames hide; the two REVIEW frames remain rendered in the burst.
    expect(page.locator(".photo-card")).to_have_count(2)

    burst_button = page.get_by_test_id("reject-burst")
    burst_button.click()

    expect(page.locator("#undoMsg")).to_have_text("Rejected 2 photos in burst")
    expect(burst_button).to_have_attribute("aria-label", "Clear rejects")
    # The hidden KEEP frames must be untouched; only the visible REVIEW frames
    # are rejected.
    assert _flags(db, photo_ids) == ["none", "none", "rejected", "rejected"]


def test_clear_rejects_reads_live_db_flags(live_server, page):
    """`Clear rejects` must not overwrite a photo the user picked live in
    Browse (another tab) just because the client cache still shows it as
    rejected. Regression: the bulk action derived changedIds and
    previousFlags from pipelineResults.photos[].flag — a client-side cache
    that page-init refreshes on load but that goes stale the moment a
    parallel session mutates flags — so a subsequent "Clear rejects" click
    silently cleared the live pick and Undo restored the stale 'rejected'
    rather than the pick."""
    db = live_server["db"]
    photo_ids = live_server["data"]["photos"][:4]
    _write_grouped_pipeline_cache(live_server, photo_ids)

    page.goto(f"{live_server['url']}/pipeline/review")
    burst_buttons = page.get_by_test_id("reject-burst")
    expect(burst_buttons).to_have_count(2)

    # Reject the first burst: both photos become 'rejected' in the DB and in
    # the in-page pipelineResults cache. The button flips to "Clear rejects".
    burst_buttons.first.click()
    expect(burst_buttons.first).to_have_attribute("aria-label", "Clear rejects")
    assert _flags(db, photo_ids) == ["rejected", "rejected", "none", "none"]

    # Simulate a live pick made in another Browse tab: the DB updates but the
    # already-rendered pipelineResults cache does not. The bulk button still
    # reads "Clear rejects" even though the first photo is now a pick.
    db.update_photo_flag(photo_ids[0], "flagged")
    expect(burst_buttons.first).to_have_attribute("aria-label", "Clear rejects")

    burst_buttons.first.click()

    # Only the truly-rejected photo in the burst is cleared. The live pick
    # is preserved; the second burst is untouched.
    expect(page.locator("#undoMsg")).to_have_text(
        "Cleared rejects from 1 photo in burst"
    )
    assert _flags(db, photo_ids) == ["flagged", "none", "none", "none"]

    page.locator("#undoToast .undo-toast-btn").click()

    expect(
        page.get_by_text("Restored previous flags for burst", exact=True)
    ).to_be_visible()
    # Undo restores what was actually in the DB when the bulk action ran —
    # the second photo goes back to 'rejected', and the live pick stays a
    # pick rather than being clobbered by a stale cached value.
    assert _flags(db, photo_ids) == ["flagged", "rejected", "none", "none"]


def test_burst_reject_blocked_while_encounter_reject_pending(live_server, page):
    """When an encounter reject is still writing, clicking a burst reject
    inside it must be blocked — otherwise both requests snapshot
    previousFlags from the pre-first-action DB read, and the burst's later
    Undo could restore the shared photos to 'none', silently rolling back
    part of the encounter reject."""
    db = live_server["db"]
    photo_ids = live_server["data"]["photos"][:4]
    _write_grouped_pipeline_cache(live_server, photo_ids)

    page.goto(f"{live_server['url']}/pipeline/review")
    burst_buttons = page.get_by_test_id("reject-burst")
    expect(burst_buttons).to_have_count(2)
    encounter_button = page.get_by_test_id("reject-encounter")

    # Hold the encounter's /api/pipeline/group/state request in flight so
    # its bulk action keeps the lock while we try to fire a burst-level
    # bulk action. Only hold the first call — post-release runs and the
    # test cleanup issue further live reads that must pass through.
    held = {}

    def handle_group_state(route):
        if "route" not in held:
            held["route"] = route
            return
        route.continue_()

    page.route("**/api/pipeline/group/state", handle_group_state)

    encounter_button.click()

    deadline = time.time() + 5
    while "route" not in held and time.time() < deadline:
        page.wait_for_timeout(50)
    assert "route" in held, "expected the encounter group-state read to be held"

    # Clicking a burst reject inside the still-writing encounter must be
    # blocked with a user-visible toast, not silently kick off a second
    # bulk action.
    burst_buttons.first.click()
    expect(
        page.get_by_text(
            "Another bulk reject is still finishing", exact=False
        )
    ).to_be_visible()
    # The blocked burst click must not have altered any DB flags: the
    # encounter's write is still gated on the held read.
    assert _flags(db, photo_ids) == ["none"] * 4

    # Release the encounter read so its bulk write can proceed against the
    # real endpoint (the batch-flag call is not intercepted).
    held["route"].continue_()

    expect(page.locator("#undoMsg")).to_have_text("Rejected 4 photos in encounter")
    assert _flags(db, photo_ids) == ["rejected"] * 4


def test_single_photo_flag_blocked_while_bulk_reject_pending(live_server, page):
    """The shared lightbox/per-photo flag path must honor the same photo-ID
    lock as overlapping bulk actions. Otherwise a pick made while the bulk
    request is paused can be overwritten by the eventual batch write and its
    Undo restores the pre-pick snapshot."""
    db = live_server["db"]
    photo_ids = live_server["data"]["photos"][:4]
    _write_grouped_pipeline_cache(live_server, photo_ids)

    page.goto(f"{live_server['url']}/pipeline/review")
    encounter_button = page.get_by_test_id("reject-encounter")
    held = {}

    def handle_group_state(route):
        if "route" not in held:
            held["route"] = route
            return
        route.continue_()

    page.route("**/api/pipeline/group/state", handle_group_state)
    encounter_button.click()

    deadline = time.time() + 5
    while "route" not in held and time.time() < deadline:
        page.wait_for_timeout(50)
    assert "route" in held, "expected the encounter group-state read to be held"

    page.evaluate(
        "([photoId]) => window.setFlagFor(photoId, 'flagged')",
        [photo_ids[0]],
    )

    expect(
        page.get_by_text(
            "A bulk reject for this photo is still finishing", exact=False
        )
    ).to_be_visible()
    assert _flags(db, photo_ids) == ["none"] * 4

    held["route"].continue_()

    expect(page.locator("#undoMsg")).to_have_text("Rejected 4 photos in encounter")
    assert _flags(db, photo_ids) == ["rejected"] * 4


def test_single_photo_flag_blocked_while_bulk_undo_pending(live_server, page):
    """Undo restores are bulk writes too, so they must retain the photo-ID
    lock until every prior flag has been restored."""
    db = live_server["db"]
    photo_ids = live_server["data"]["photos"][:4]
    _write_grouped_pipeline_cache(live_server, photo_ids)

    page.goto(f"{live_server['url']}/pipeline/review")
    burst_button = page.get_by_test_id("reject-burst").first
    burst_button.click()
    expect(page.locator("#undoMsg")).to_have_text("Rejected 2 photos in burst")
    assert _flags(db, photo_ids) == ["rejected", "rejected", "none", "none"]

    held = {}

    def handle_batch_flag(route):
        if "route" not in held:
            held["route"] = route
            return
        route.continue_()

    page.route("**/api/batch/flag", handle_batch_flag)
    page.locator("#undoToast .undo-toast-btn").click()

    deadline = time.time() + 5
    while "route" not in held and time.time() < deadline:
        page.wait_for_timeout(50)
    assert "route" in held, "expected the undo batch write to be held"

    page.evaluate(
        "([photoId]) => window.setFlagFor(photoId, 'flagged')",
        [photo_ids[0]],
    )

    expect(
        page.get_by_text(
            "A bulk reject for this photo is still finishing", exact=False
        )
    ).to_be_visible()
    assert _flags(db, photo_ids) == ["rejected", "rejected", "none", "none"]

    held["route"].continue_()

    expect(
        page.get_by_text("Restored previous flags for burst", exact=True)
    ).to_be_visible()
    assert _flags(db, photo_ids) == ["none"] * 4


def test_bulk_reject_surfaces_live_flag_read_failure(live_server, page):
    """A failed live-state read must use the standard request error toast
    instead of silently abandoning the group action."""
    db = live_server["db"]
    photo_ids = live_server["data"]["photos"][:4]
    _write_grouped_pipeline_cache(live_server, photo_ids)

    page.goto(f"{live_server['url']}/pipeline/review")
    page.route(
        "**/api/pipeline/group/state",
        lambda route: route.fulfill(
            status=500,
            json={"error": "Could not read current photo flags"},
        ),
    )

    page.get_by_test_id("reject-burst").first.click()

    expect(
        page.get_by_text("Could not read current photo flags", exact=True)
    ).to_be_visible()
    assert _flags(db, photo_ids) == ["none"] * 4


def test_encounter_reject_respects_active_label_filter(live_server, page):
    """Same guarantee as the burst-level test, but for the encounter-level
    Reject/Clear button: hidden KEEP frames stay untouched when the Review
    filter is active."""
    db = live_server["db"]
    photo_ids = live_server["data"]["photos"][:4]
    _write_mixed_label_pipeline_cache(live_server, photo_ids)

    page.goto(f"{live_server['url']}/pipeline/review")
    page.locator('.filter-btn[data-filter="REVIEW"]').click()
    expect(page.locator(".photo-card")).to_have_count(2)

    encounter_button = page.get_by_test_id("reject-encounter")
    encounter_button.click()

    expect(page.locator("#undoMsg")).to_have_text("Rejected 2 photos in encounter")
    expect(encounter_button).to_have_attribute("aria-label", "Clear rejects")
    assert _flags(db, photo_ids) == ["none", "none", "rejected", "rejected"]

    encounter_button.click()

    expect(encounter_button).to_have_attribute("aria-label", "Reject encounter")
    expect(page.locator("#undoMsg")).to_have_text(
        "Cleared rejects from 2 photos in encounter"
    )
    assert _flags(db, photo_ids) == ["none"] * 4
