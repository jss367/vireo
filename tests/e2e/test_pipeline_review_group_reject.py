"""Process-review controls for rejecting a burst or full encounter."""

import json
import os
import re
import time

import pytest
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

    expect(page.locator("#undoMsg")).to_have_text("Set flag to rejected on 2 photos")
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
    expect(page.get_by_text("Undone: Set flag to rejected on 2 photos", exact=True)).to_be_visible()
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
    expect(page.locator("#undoMsg")).to_have_text("Set flag to rejected on 4 photos")
    assert _flags(db, photo_ids) == ["rejected"] * 4

    encounter_button.click()

    expect(encounter_button).to_have_attribute("aria-label", "Reject encounter")
    expect(page.locator("#undoMsg")).to_have_text(
        "Set flag to none on 4 photos"
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
    expect(page.locator("#undoMsg")).to_have_text("Set flag to rejected on 2 photos")
    # First (confirmed, hidden) burst must be untouched; only the visible
    # unconfirmed burst's photos are rejected.
    assert _flags(db, photo_ids) == ["none", "none", "rejected", "rejected"]

    encounter_button.click()

    expect(encounter_button).to_have_attribute("aria-label", "Reject encounter")
    expect(page.locator("#undoMsg")).to_have_text(
        "Set flag to none on 2 photos"
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

    expect(page.locator("#undoMsg")).to_have_text("Set flag to rejected on 2 photos")
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
        "Set flag to none on 1 photos"
    )
    assert _flags(db, photo_ids) == ["flagged", "none", "none", "none"]

    page.locator("#undoToast .undo-toast-btn").click()

    expect(
        page.get_by_text("Undone: Set flag to none on 1 photos", exact=True)
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

    expect(page.locator("#undoMsg")).to_have_text("Set flag to rejected on 4 photos")
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

    landed = page.evaluate(
        "([photoId]) => window.setFlagFor(photoId, 'flagged')",
        [photo_ids[0]],
    )

    assert landed is False
    expect(
        page.get_by_text(
            "A bulk reject for this photo is still finishing", exact=False
        )
    ).to_be_visible()
    assert _flags(db, photo_ids) == ["none"] * 4

    held["route"].continue_()

    expect(page.locator("#undoMsg")).to_have_text("Set flag to rejected on 4 photos")
    assert _flags(db, photo_ids) == ["rejected"] * 4


def test_burst_modal_apply_blocked_while_bulk_reject_pending(live_server, page):
    """The burst review modal writes flags through its own group/apply route,
    so it must consult the shared photo-ID lock independently."""
    db = live_server["db"]
    photo_ids = live_server["data"]["photos"][:4]
    _write_grouped_pipeline_cache(live_server, photo_ids)

    page.goto(f"{live_server['url']}/pipeline/review")
    held = {}

    def handle_group_state(route):
        if "route" not in held:
            held["route"] = route
            return
        route.continue_()

    page.route("**/api/pipeline/group/state", handle_group_state)
    page.get_by_test_id("reject-burst").first.click()

    deadline = time.time() + 5
    while "route" not in held and time.time() < deadline:
        page.wait_for_timeout(50)
    assert "route" in held, "expected the bulk live-state read to be held"

    # The grid remains interactive while the bulk read is pending. Open the
    # same burst in the modal, make a reject decision, and try to apply it.
    page.locator(f'.photo-card[data-photo-id="{photo_ids[0]}"] img').click()
    expect(page.locator("#grmApplyBtn")).to_be_enabled()
    page.keyboard.press("x")
    expect(page.locator("#grmApplyFlagsChk")).to_be_checked()

    apply_requests = []
    page.on(
        "request",
        lambda request: (
            apply_requests.append(request)
            if "/api/pipeline/group/apply" in request.url
            else None
        ),
    )
    page.locator("#grmApplyBtn").click()

    expect(
        page.get_by_text(
            "A bulk reject for this group is still finishing", exact=False
        )
    ).to_be_visible()
    expect(page.locator("#grmOverlay")).to_have_class(re.compile(r"\bopen\b"))
    expect(page.locator("#grmApplyBtn")).to_be_enabled()
    assert apply_requests == []
    assert _flags(db, photo_ids) == ["none"] * 4

    # The native Photo > Pick command must use the page's batchSetFlag helper
    # rather than bypassing the modal through /api/batch/flag. The pending
    # reject stays put and no database write lands while the bulk lock is held.
    page.evaluate("() => nativeMenuSetFlag('flagged')")
    expect(
        page.get_by_text(
            "A bulk reject for the selected photos is still finishing",
            exact=False,
        )
    ).to_be_visible()
    expect(page.locator("#grmCount")).to_have_text(
        "0 picks, 1 rejects, 1 unsorted"
    )
    assert _flags(db, photo_ids) == ["none"] * 4

    held["route"].continue_()

    expect(page.locator("#undoMsg")).to_have_text("Set flag to rejected on 2 photos")
    assert _flags(db, photo_ids) == ["rejected", "rejected", "none", "none"]


def test_single_photo_flag_blocked_while_bulk_undo_pending(live_server, page):
    """Undo restores are bulk writes too, so they must retain the photo-ID
    lock until every prior flag has been restored."""
    db = live_server["db"]
    photo_ids = live_server["data"]["photos"][:4]
    _write_grouped_pipeline_cache(live_server, photo_ids)

    page.goto(f"{live_server['url']}/pipeline/review")
    burst_button = page.get_by_test_id("reject-burst").first
    burst_button.click()
    expect(page.locator("#undoMsg")).to_have_text("Set flag to rejected on 2 photos")
    assert _flags(db, photo_ids) == ["rejected", "rejected", "none", "none"]

    held = {}

    def handle_batch_flag(route):
        if "route" not in held:
            held["route"] = route
            return
        route.continue_()

    page.route("**/api/undo", handle_batch_flag)
    page.locator("#undoToast .undo-toast-btn").click()

    deadline = time.time() + 5
    while "route" not in held and time.time() < deadline:
        page.wait_for_timeout(50)
    assert "route" in held, "expected the undo request to be held"

    page.evaluate(
        "([photoId]) => window.setFlagFor(photoId, 'flagged')",
        [photo_ids[0]],
    )

    expect(
        page.get_by_text(
            "Undo or redo is still finishing", exact=False
        )
    ).to_be_visible()
    assert _flags(db, photo_ids) == ["rejected", "rejected", "none", "none"]

    held["route"].continue_()

    expect(
        page.get_by_text("Undone: Set flag to rejected on 2 photos", exact=True)
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

    expect(page.locator("#undoMsg")).to_have_text("Set flag to rejected on 2 photos")
    expect(encounter_button).to_have_attribute("aria-label", "Clear rejects")
    assert _flags(db, photo_ids) == ["none", "none", "rejected", "rejected"]

    encounter_button.click()

    expect(encounter_button).to_have_attribute("aria-label", "Reject encounter")
    expect(page.locator("#undoMsg")).to_have_text(
        "Set flag to none on 2 photos"
    )
    assert _flags(db, photo_ids) == ["none"] * 4


def test_photo_context_menu_exposes_review_and_organization_actions(
    live_server, page
):
    photo_ids = live_server["data"]["photos"][:4]
    _write_grouped_pipeline_cache(live_server, photo_ids)

    page.goto(f"{live_server['url']}/pipeline/review")
    page.locator(".photo-card[data-photo-id]").first.click(button="right")

    menu = page.locator(".vireo-ctx-menu")
    expect(menu).to_be_visible()
    menu_text = menu.inner_text()
    for label in (
        "Add to Collection",
        "Add Keyword",
        "Open in Lightbox",
        "Open in Browse",
        "Find Similar",
        "Copy Path",
        "Edit Photo",
        "Open in Editor",
        "Develop in darktable",
    ):
        assert label in menu_text
    expect(menu.locator('.vireo-ctx-chip[title="Rate 4"]')).to_have_count(1)
    expect(menu.locator('.vireo-ctx-chip[title="Flag as pick"]')).to_have_count(1)
    expect(menu.locator('.vireo-ctx-chip[title="Reject"]')).to_have_count(1)
    expect(menu.locator('.vireo-ctx-chip[title="Clear flag"]')).to_have_count(1)


def test_lightbox_navigation_follows_visible_filtered_cards(live_server, page):
    photo_ids = live_server["data"]["photos"][:4]
    _write_mixed_label_pipeline_cache(live_server, photo_ids)

    page.goto(f"{live_server['url']}/pipeline/review")
    page.locator('.filter-btn[data-filter="REVIEW"]').click()
    cards = page.locator("#encountersContainer .photo-card[data-photo-id]")
    expect(cards).to_have_count(2)
    visible_ids = [
        int(cards.nth(i).get_attribute("data-photo-id")) for i in range(cards.count())
    ]

    page.evaluate("pid => openPipelineLightbox(pid)", visible_ids[0])
    assert page.evaluate("_lightboxPhotoList.map(photo => photo.id)") == visible_ids
    page.keyboard.press("ArrowRight")
    assert page.evaluate("_lightboxCurrentId") == visible_ids[1]

    page.evaluate("closeLightbox()")
    page.evaluate(
        "pid => setTimeout(function() { openPipelinePhotoEditor(pid); }, 0)",
        visible_ids[0],
    )
    page.wait_for_url(f"**/edit/{visible_ids[0]}")
    assert page.evaluate("window.vireoEditNav.getList()") == visible_ids


def test_native_photo_command_opens_pipeline_lightbox(live_server, page):
    photo_ids = live_server["data"]["photos"][:4]
    _write_grouped_pipeline_cache(live_server, photo_ids)

    page.goto(f"{live_server['url']}/pipeline/review")
    expect(page.locator(".photo-card[data-photo-id]")).to_have_count(4)
    photo_id = photo_ids[0]
    page.evaluate(
        "pid => { pipelineReviewContextPhotoIds = [pid]; nativeMenuOpenLightbox(); }",
        photo_id,
    )
    expect(page.locator("#lightboxOverlay")).to_have_class(re.compile(r"\bactive\b"))
    assert page.evaluate("_lightboxCurrentId") == photo_id


def test_photo_context_menu_adds_photo_to_existing_collection(live_server, page):
    db = live_server["db"]
    photo_ids = live_server["data"]["photos"][:4]
    _write_grouped_pipeline_cache(live_server, photo_ids)
    collection_id = db.add_collection(
        "Process Review Picks",
        json.dumps([{"field": "photo_ids", "value": []}]),
    )

    page.goto(f"{live_server['url']}/pipeline/review")
    card = page.locator(".photo-card[data-photo-id]").first
    photo_id = int(card.get_attribute("data-photo-id"))
    card.click(button="right")
    page.locator(".vireo-ctx-item", has_text="Add to Collection").click()

    modal = page.locator("#pipelineCollectionModal")
    expect(modal).to_have_class(re.compile(r"\bopen\b"))
    modal.locator(".pipeline-collection-choice", has_text="Process Review Picks").click()
    modal.locator(".modal-btn-primary", has_text="Add").click()
    expect(modal).not_to_have_class(re.compile(r"\bopen\b"))

    members = db.get_collection_photos(collection_id, per_page=100)
    assert [member["id"] for member in members] == [photo_id]


def test_collection_load_failure_clears_native_selection_override(live_server, page):
    photo_ids = live_server["data"]["photos"][:4]
    _write_grouped_pipeline_cache(live_server, photo_ids)

    page.goto(f"{live_server['url']}/pipeline/review")
    expect(page.locator(".photo-card[data-photo-id]")).to_have_count(4)
    state = page.evaluate(
        """async ids => {
          const originalSafeFetch = window.safeFetch;
          window._vireoNativeMenuPhotoIdsOverride = ids.slice(0, 2);
          window.safeFetch = function(url) {
            if (url === '/api/collections') return Promise.reject(new Error('offline'));
            return originalSafeFetch.apply(this, arguments);
          };
          try {
            await addToCollection();
            return {
              override: window._vireoNativeMenuPhotoIdsOverride,
              modalIds: pipelineReviewCollectionPhotoIds.slice(),
              modalOpen: document.getElementById('pipelineCollectionModal').classList.contains('open'),
            };
          } finally {
            window.safeFetch = originalSafeFetch;
          }
        }""",
        photo_ids,
    )
    assert state == {"override": None, "modalIds": [], "modalOpen": False}


def test_organization_actions_release_native_selection_after_capture(live_server, page):
    photo_ids = live_server["data"]["photos"][:4]
    _write_grouped_pipeline_cache(live_server, photo_ids)

    page.goto(f"{live_server['url']}/pipeline/review")
    expect(page.locator(".photo-card[data-photo-id]")).to_have_count(4)
    state = page.evaluate(
        """async ids => {
          const originalSafeFetch = window.safeFetch;
          let releaseCollections;
          window.safeFetch = function(url) {
            if (url === '/api/collections') {
              return new Promise(resolve => { releaseCollections = resolve; });
            }
            return originalSafeFetch.apply(this, arguments);
          };
          try {
            window._vireoNativeMenuPhotoIdsOverride = [ids[0]];
            const collectionOpen = addToCollection();
            pipelineReviewContextPhotoIds = [ids[1]];
            const whileLoading = {
              override: window._vireoNativeMenuPhotoIdsOverride,
              activeSelection: getActiveSelection(),
            };
            releaseCollections([]);
            await collectionOpen;

            window._vireoNativeMenuPhotoIdsOverride = [ids[2]];
            batchAddKeyword();
            return {
              whileLoading: whileLoading,
              collectionIds: pipelineReviewCollectionPhotoIds.slice(),
              collectionOverride: window._vireoNativeMenuPhotoIdsOverride,
              keywordIds: pipelineReviewKeywordPhotoIds.slice(),
            };
          } finally {
            window.safeFetch = originalSafeFetch;
          }
        }""",
        photo_ids,
    )
    assert state == {
        "whileLoading": {"override": None, "activeSelection": [photo_ids[1]]},
        "collectionIds": [photo_ids[0]],
        "collectionOverride": None,
        "keywordIds": [photo_ids[2]],
    }


def test_latest_collection_load_owns_modal_selection(live_server, page):
    photo_ids = live_server["data"]["photos"][:4]
    _write_grouped_pipeline_cache(live_server, photo_ids)

    page.goto(f"{live_server['url']}/pipeline/review")
    expect(page.locator(".photo-card[data-photo-id]")).to_have_count(4)
    state = page.evaluate(
        """async ids => {
          const originalSafeFetch = window.safeFetch;
          const resolvers = [];
          window.safeFetch = function(url) {
            if (url === '/api/collections') {
              return new Promise(resolve => { resolvers.push(resolve); });
            }
            return originalSafeFetch.apply(this, arguments);
          };
          try {
            const first = addToCollection([ids[0]]);
            const secondIds = [ids[1], ids[2]];
            const second = addToCollection(secondIds);
            resolvers[0]([]);
            const firstResult = await first;
            const afterFirst = {
              open: document.getElementById('pipelineCollectionModal').classList.contains('open'),
              ids: pipelineReviewCollectionPhotoIds.slice(),
            };
            resolvers[1]([]);
            const secondResult = await second;
            return {
              firstResult,
              secondResult,
              afterFirst,
              finalIds: pipelineReviewCollectionPhotoIds.slice(),
              title: document.getElementById('pipelineCollectionTitle').textContent,
            };
          } finally {
            window.safeFetch = originalSafeFetch;
          }
        }""",
        photo_ids,
    )
    assert state == {
        "firstResult": False,
        "secondResult": True,
        "afterFirst": {"open": False, "ids": []},
        "finalIds": photo_ids[1:3],
        "title": "Add 2 photos to Collection",
    }


def test_new_collection_submission_is_single_flight_and_retryable(live_server, page):
    photo_ids = live_server["data"]["photos"][:4]
    _write_grouped_pipeline_cache(live_server, photo_ids)

    page.goto(f"{live_server['url']}/pipeline/review")
    expect(page.locator(".photo-card[data-photo-id]")).to_have_count(4)
    state = page.evaluate(
        """async ids => {
          const originalSafeFetch = window.safeFetch;
          let createCalls = 0;
          let addCalls = 0;
          let releaseCreate;
          const createGate = new Promise(resolve => { releaseCreate = resolve; });
          window.safeFetch = async function(url, options) {
            if (url === '/api/collections' && options && options.method === 'POST') {
              createCalls += 1;
              await createGate;
              return {id: 9876};
            }
            if (url === '/api/collections/9876/add-photos') {
              addCalls += 1;
              if (addCalls === 1) throw new Error('temporary add failure');
              return {ok: true};
            }
            return originalSafeFetch.apply(this, arguments);
          };
          pipelineReviewCollectionPhotoIds = ids.slice(0, 2);
          document.getElementById('pipelineCollectionNewName').value = 'Retry Picks';
          document.getElementById('pipelineCollectionModal').classList.add('open');
          try {
            const first = confirmPipelineCollection();
            const duplicate = confirmPipelineCollection();
            const disabledWhilePending = document.getElementById('pipelineCollectionSubmitBtn').disabled;
            releaseCreate();
            const firstResults = await Promise.all([first, duplicate]);
            const retainedId = pipelineReviewCreatedCollection && pipelineReviewCreatedCollection.id;
            const retryResult = await confirmPipelineCollection();
            return {createCalls, addCalls, disabledWhilePending, firstResults, retainedId, retryResult};
          } finally {
            window.safeFetch = originalSafeFetch;
          }
        }""",
        photo_ids,
    )
    assert state == {
        "createCalls": 1,
        "addCalls": 2,
        "disabledWhilePending": True,
        "firstResults": [False, False],
        "retainedId": 9876,
        "retryResult": True,
    }


def test_collection_submission_blocks_close_and_reopen(live_server, page):
    photo_ids = live_server["data"]["photos"][:4]
    _write_grouped_pipeline_cache(live_server, photo_ids)

    page.goto(f"{live_server['url']}/pipeline/review")
    state = page.evaluate(
        """async ids => {
          const originalSafeFetch = window.safeFetch;
          let release;
          window.safeFetch = function(url) {
            if (url === '/api/collections/321/add-photos') {
              return new Promise(resolve => { release = resolve; });
            }
            return originalSafeFetch.apply(this, arguments);
          };
          pipelineReviewCollectionPhotoIds = [ids[0]];
          pipelineReviewSelectedCollectionId = 321;
          pipelineReviewCollections = [{id: 321, name: 'Existing Picks'}];
          document.getElementById('pipelineCollectionModal').classList.add('open');
          try {
            const submission = confirmPipelineCollection();
            const closeResult = hidePipelineCollectionModal();
            const reopenResult = await addToCollection([ids[1]]);
            const pending = {
              closeResult,
              reopenResult,
              open: document.getElementById('pipelineCollectionModal').classList.contains('open'),
              ids: pipelineReviewCollectionPhotoIds.slice(),
              cancelDisabled: document.getElementById('pipelineCollectionCancelBtn').disabled,
            };
            release({ok: true});
            const submitResult = await submission;
            return {
              pending,
              submitResult,
              openAfter: document.getElementById('pipelineCollectionModal').classList.contains('open'),
            };
          } finally {
            window.safeFetch = originalSafeFetch;
          }
        }""",
        photo_ids,
    )
    assert state == {
        "pending": {
            "closeResult": False,
            "reopenResult": False,
            "open": True,
            "ids": [photo_ids[0]],
            "cancelDisabled": True,
        },
        "submitResult": True,
        "openAfter": False,
    }


def test_keyword_submission_blocks_close_and_reopen(live_server, page):
    photo_ids = live_server["data"]["photos"][:4]
    _write_grouped_pipeline_cache(live_server, photo_ids)

    page.goto(f"{live_server['url']}/pipeline/review")
    state = page.evaluate(
        """async ids => {
          const originalSafeFetch = window.safeFetch;
          let release;
          window.safeFetch = function(url) {
            if (url === '/api/batch/keyword') {
              return new Promise(resolve => { release = resolve; });
            }
            return originalSafeFetch.apply(this, arguments);
          };
          pipelineReviewKeywordPhotoIds = [ids[0]];
          document.getElementById('pipelineKeywordInput').value = 'First Keyword';
          document.getElementById('pipelineKeywordModal').classList.add('open');
          try {
            const submission = confirmPipelineKeyword();
            const closeResult = hidePipelineKeywordModal();
            const reopenResult = batchAddKeyword([ids[1]]);
            const pending = {
              closeResult,
              reopenResult,
              open: document.getElementById('pipelineKeywordModal').classList.contains('open'),
              ids: pipelineReviewKeywordPhotoIds.slice(),
              cancelDisabled: document.getElementById('pipelineKeywordCancelBtn').disabled,
            };
            release({ok: true});
            const submitResult = await submission;
            return {
              pending,
              submitResult,
              openAfter: document.getElementById('pipelineKeywordModal').classList.contains('open'),
            };
          } finally {
            window.safeFetch = originalSafeFetch;
          }
        }""",
        photo_ids,
    )
    assert state == {
        "pending": {
            "closeResult": False,
            "reopenResult": False,
            "open": True,
            "ids": [photo_ids[0]],
            "cancelDisabled": True,
        },
        "submitResult": True,
        "openAfter": False,
    }


def test_collection_and_keyword_dialogs_have_separate_selections(live_server, page):
    photo_ids = live_server["data"]["photos"][:4]
    _write_grouped_pipeline_cache(live_server, photo_ids)

    page.goto(f"{live_server['url']}/pipeline/review")
    expect(page.locator(".photo-card[data-photo-id]")).to_have_count(4)
    state = page.evaluate(
        """async ids => {
          const originalSafeFetch = window.safeFetch;
          let releaseCollectionAdd;
          let releaseKeyword;
          let addCallIds = null;
          let keywordCallIds = null;
          window.safeFetch = function(url, options) {
            if (url === '/api/collections') {
              return Promise.resolve([{id: 555, name: 'Reviewer Picks',
                rules: [{field: 'photo_ids', value: []}]}]);
            }
            if (url === '/api/collections/555/add-photos') {
              addCallIds = JSON.parse(options.body).photo_ids.slice();
              return new Promise(resolve => { releaseCollectionAdd = resolve; });
            }
            if (url === '/api/batch/keyword') {
              keywordCallIds = JSON.parse(options.body).photo_ids.slice();
              return new Promise(resolve => { releaseKeyword = resolve; });
            }
            return originalSafeFetch.apply(this, arguments);
          };
          try {
            // Open Add to Collection for the first selection and pick a
            // collection so the dialog is fully configured.
            const collectionOpen = await addToCollection([ids[0], ids[1]]);
            pickPipelineCollection(555);
            const afterCollectionOpen = {
              collectionIds: pipelineReviewCollectionPhotoIds.slice(),
              keywordIds: pipelineReviewKeywordPhotoIds.slice(),
            };

            // Open Add Keyword for a different selection while the collection
            // dialog is still open. This must not overwrite the collection
            // dialog's captured photo IDs.
            batchAddKeyword([ids[2], ids[3]]);
            const afterKeywordOpen = {
              collectionIds: pipelineReviewCollectionPhotoIds.slice(),
              keywordIds: pipelineReviewKeywordPhotoIds.slice(),
              collectionOpen:
                document.getElementById('pipelineCollectionModal').classList.contains('open'),
              keywordOpen:
                document.getElementById('pipelineKeywordModal').classList.contains('open'),
            };

            // Closing the keyword dialog must not clear the collection dialog's
            // captured IDs.
            hidePipelineKeywordModal();
            const afterKeywordClose = {
              collectionIds: pipelineReviewCollectionPhotoIds.slice(),
              keywordIds: pipelineReviewKeywordPhotoIds.slice(),
              collectionOpen:
                document.getElementById('pipelineCollectionModal').classList.contains('open'),
            };

            // Reopen the keyword dialog with yet another selection and confirm
            // both dialogs. Each request must carry its own captured IDs.
            batchAddKeyword([ids[3]]);
            document.getElementById('pipelineKeywordInput').value = 'Standalone';
            const submitKeyword = confirmPipelineKeyword();
            const submitCollection = confirmPipelineCollection();
            releaseCollectionAdd({ok: true});
            releaseKeyword({ok: true});
            const [collectionResult, keywordResult] =
              await Promise.all([submitCollection, submitKeyword]);
            return {
              collectionOpen,
              afterCollectionOpen,
              afterKeywordOpen,
              afterKeywordClose,
              addCallIds,
              keywordCallIds,
              collectionResult,
              keywordResult,
            };
          } finally {
            window.safeFetch = originalSafeFetch;
          }
        }""",
        photo_ids,
    )
    assert state == {
        "collectionOpen": True,
        "afterCollectionOpen": {
            "collectionIds": photo_ids[:2],
            "keywordIds": [],
        },
        "afterKeywordOpen": {
            "collectionIds": photo_ids[:2],
            "keywordIds": photo_ids[2:4],
            "collectionOpen": True,
            "keywordOpen": True,
        },
        "afterKeywordClose": {
            "collectionIds": photo_ids[:2],
            "keywordIds": [],
            "collectionOpen": True,
        },
        "addCallIds": photo_ids[:2],
        "keywordCallIds": [photo_ids[3]],
        "collectionResult": True,
        "keywordResult": True,
    }


def test_newest_organization_dialog_is_frontmost_and_owns_escape(live_server, page):
    photo_ids = live_server["data"]["photos"][:4]
    _write_grouped_pipeline_cache(live_server, photo_ids)

    page.goto(f"{live_server['url']}/pipeline/review")
    expect(page.locator(".photo-card[data-photo-id]")).to_have_count(4)
    assert page.evaluate("ids => addToCollection([ids[0]])", photo_ids) is True
    page.evaluate("ids => batchAddKeyword([ids[1]])", photo_ids)

    collection = page.locator("#pipelineCollectionModal")
    keyword = page.locator("#pipelineKeywordModal")
    expect(collection).to_have_class(re.compile(r"\bopen\b"))
    expect(keyword).to_have_class(re.compile(r"\bopen\b"))
    stack = page.evaluate(
        "() => ({"
        "active: pipelineReviewActiveOrganizationModal,"
        "collection: Number(document.getElementById('pipelineCollectionModal').style.zIndex),"
        "keyword: Number(document.getElementById('pipelineKeywordModal').style.zIndex),"
        "})"
    )
    assert stack == {"active": "keyword", "collection": 550, "keyword": 551}

    page.keyboard.press("Escape")
    expect(keyword).not_to_have_class(re.compile(r"\bopen\b"))
    expect(collection).to_have_class(re.compile(r"\bopen\b"))
    assert page.evaluate("pipelineReviewActiveOrganizationModal") == "collection"


def test_photo_context_menu_updates_rating_and_adds_keyword(live_server, page):
    db = live_server["db"]
    photo_ids = live_server["data"]["photos"][:4]
    _write_grouped_pipeline_cache(live_server, photo_ids)

    page.goto(f"{live_server['url']}/pipeline/review")
    card = page.locator(".photo-card[data-photo-id]").first
    photo_id = int(card.get_attribute("data-photo-id"))
    card.click(button="right")
    page.locator('.vireo-ctx-chip[title="Rate 4"]').click()

    rating = db.conn.execute(
        "SELECT rating FROM photos WHERE id = ?", (photo_id,)
    ).fetchone()["rating"]
    assert rating == 4

    card.click(button="right")
    page.locator(".vireo-ctx-item", has_text="Add Keyword").click()
    modal = page.locator("#pipelineKeywordModal")
    expect(modal).to_have_class(re.compile(r"\bopen\b"))
    modal.locator("#pipelineKeywordInput").fill("Process Review Shortlist")
    modal.locator(".modal-btn-primary", has_text="Add").click()
    expect(modal).not_to_have_class(re.compile(r"\bopen\b"))

    assert "Process Review Shortlist" in {
        keyword["name"] for keyword in db.get_photo_keywords(photo_id)
    }


def test_group_context_menu_preserves_selection_for_collection_action(
    live_server, page
):
    db = live_server["db"]
    photo_ids = live_server["data"]["photos"][:4]
    _write_grouped_pipeline_cache(live_server, photo_ids)
    collection_id = db.add_collection(
        "Burst Shortlist",
        json.dumps([{"field": "photo_ids", "value": []}]),
    )

    page.goto(f"{live_server['url']}/pipeline/review")
    expect(page.locator(".photo-card[data-photo-id]")).to_have_count(4)
    page.evaluate("openGroupReview(0, 0)")
    cards = page.locator("#grmOverlay .grm-card[data-photo-id]")
    expect(cards).to_have_count(2)
    selected_ids = [
        int(cards.nth(0).get_attribute("data-photo-id")),
        int(cards.nth(1).get_attribute("data-photo-id")),
    ]
    # The modal opens with the first card selected. Add the second without
    # toggling the first one off through grmSelect's click-again behavior.
    page.evaluate("id => grmSelect(id, 'toggle')", selected_ids[1])
    cards.nth(1).click(button="right")

    menu = page.locator(".vireo-ctx-menu")
    for label in (
        "Move to Picks",
        "Move to Candidates",
        "Move to Rejects",
        "Remove from Group",
    ):
        expect(menu.locator(".vireo-ctx-item", has_text=label)).to_be_visible()
    menu.locator(".vireo-ctx-item", has_text="Add to Collection").click()

    modal = page.locator("#pipelineCollectionModal")
    expect(modal.locator("#pipelineCollectionTitle")).to_have_text(
        "Add 2 photos to Collection"
    )
    modal.locator(".pipeline-collection-choice", has_text="Burst Shortlist").click()
    modal.locator(".modal-btn-primary", has_text="Add").click()
    expect(modal).not_to_have_class(re.compile(r"\bopen\b"))

    members = db.get_collection_photos(collection_id, per_page=100)
    assert {member["id"] for member in members} == set(selected_ids)


def test_group_context_menu_move_uses_selection_captured_when_opened(
    live_server, page
):
    photo_ids = live_server["data"]["photos"][:4]
    _write_grouped_pipeline_cache(live_server, photo_ids)

    page.goto(f"{live_server['url']}/pipeline/review")
    expect(page.locator(".photo-card[data-photo-id]")).to_have_count(4)
    page.evaluate("openGroupReview(0, 0)")
    page.wait_for_function("grmState && grmState.seeded === true")
    cards = page.locator("#grmOverlay .grm-card[data-photo-id]")
    captured_id = int(cards.nth(0).get_attribute("data-photo-id"))
    later_id = int(cards.nth(1).get_attribute("data-photo-id"))

    cards.nth(0).click(button="right")
    menu = page.locator(".vireo-ctx-menu")
    expect(menu).to_be_visible()
    page.keyboard.press("ArrowRight")
    expect(menu).to_be_visible()
    assert page.evaluate("grmState.selected") == later_id

    menu.locator(".vireo-ctx-item", has_text="Move to Rejects").click()
    zones = page.evaluate(
        "() => ({picks: Array.from(grmState.picks), "
        "rejects: Array.from(grmState.rejects)})"
    )
    assert captured_id in zones["rejects"]
    assert later_id not in zones["rejects"]
    assert later_id not in zones["picks"]


def test_lightbox_over_group_review_suppresses_group_shortcuts(live_server, page):
    """Space/Delete/Backspace pressed inside the shared lightbox must not
    reach the Group Review keydown handler on the hidden overlay beneath."""
    db = live_server["db"]
    photo_ids = live_server["data"]["photos"][:4]
    _write_grouped_pipeline_cache(live_server, photo_ids)

    page.goto(f"{live_server['url']}/pipeline/review")
    expect(page.locator(".photo-card[data-photo-id]")).to_have_count(4)
    page.evaluate("openGroupReview(0, 0)")
    expect(page.locator("#grmOverlay")).to_have_class(re.compile(r"\bopen\b"))
    burst_ids = photo_ids[:2]
    expect(page.locator("#grmOverlay .grm-card[data-photo-id]")).to_have_count(2)

    # Open the lightbox above the group review on the first burst photo. The
    # burst overlay stays open beneath; grmState.selected still names the
    # first photo, so a stray Space or Delete would silently move it or drop
    # it from the group.
    page.evaluate(
        "pid => openPipelineLightbox(pid)",
        burst_ids[0],
    )
    expect(page.locator("#lightboxOverlay")).to_have_class(
        re.compile(r"\bactive\b")
    )

    page.locator("#lightboxImg").press("Space")
    page.locator("#lightboxImg").press("Delete")
    page.locator("#lightboxImg").press("Backspace")

    # Group Review's pending zones and touched set must be untouched.
    zones = page.evaluate(
        "() => ({"
        "picks: Array.from(grmState.picks),"
        "rejects: Array.from(grmState.rejects),"
        "removed: Array.from(grmState.removed),"
        "touched: Array.from(grmState.touched || []),"
        "})"
    )
    assert zones == {"picks": [], "rejects": [], "removed": [], "touched": []}


def test_lightbox_browse_uses_state_preserving_navigation_helper(live_server, page):
    photo_ids = live_server["data"]["photos"][:4]
    _write_grouped_pipeline_cache(live_server, photo_ids)

    page.goto(f"{live_server['url']}/pipeline/review")
    expect(page.locator(".photo-card[data-photo-id]")).to_have_count(4)
    page.evaluate("openGroupReview(0, 0)")
    page.wait_for_function("grmState && grmState.seeded === true")
    photo_id = photo_ids[0]
    page.evaluate(
        """pid => {
          grmMoveReject();
          window.__lightboxBrowsePhotoId = null;
          window.openInBrowse = id => { window.__lightboxBrowsePhotoId = id; };
          openPipelineLightbox(pid);
        }""",
        photo_id,
    )

    page.evaluate(
        """pid => {
          const item = buildLightboxContextMenu(pid).find(
            candidate => candidate.label === 'Open in Browse'
          );
          item.onClick();
        }""",
        photo_id,
    )
    assert page.evaluate("window.__lightboxBrowsePhotoId") == photo_id
    expect(page.locator("#grmOverlay")).to_have_class(re.compile(r"\bopen\b"))
    assert page.evaluate("grmState.rejects.has(grmState.selected)") is True


def test_similar_photos_overlay_suppresses_group_shortcuts(live_server, page):
    photo_ids = live_server["data"]["photos"][:4]
    _write_grouped_pipeline_cache(live_server, photo_ids)

    page.goto(f"{live_server['url']}/pipeline/review")
    expect(page.locator(".photo-card[data-photo-id]")).to_have_count(4)
    page.evaluate("openGroupReview(0, 0)")
    page.wait_for_function("grmState && grmState.seeded === true")
    page.evaluate("findSimilar(grmState.selected)")
    expect(page.locator("#similarOverlay")).to_have_class(re.compile(r"\bactive\b"))

    page.keyboard.press("p")
    page.keyboard.press("x")
    page.keyboard.press("Space")
    page.keyboard.press("Delete")
    page.keyboard.press("Backspace")
    zones = page.evaluate(
        "() => ({"
        "picks: Array.from(grmState.picks),"
        "rejects: Array.from(grmState.rejects),"
        "removed: Array.from(grmState.removed),"
        "touched: Array.from(grmState.touched || []),"
        "})"
    )
    assert zones == {"picks": [], "rejects": [], "removed": [], "touched": []}


def test_similar_result_lightbox_preserves_scoped_read_only_mode(live_server, page):
    photo_ids = live_server["data"]["photos"][:4]
    _write_grouped_pipeline_cache(live_server, photo_ids)
    result_id = photo_ids[1]
    page.route(
        "**/api/photos/*/similar?limit=40",
        lambda route: route.fulfill(
            json={
                "total_compared": 1,
                "similar": [
                    {
                        "similarity": 0.91,
                        "photo": {"id": result_id, "filename": "similar.jpg"},
                    }
                ],
            }
        ),
    )

    page.goto(f"{live_server['url']}/pipeline/review")
    expect(page.locator(".photo-card[data-photo-id]")).to_have_count(4)
    page.evaluate(
        "pid => { reviewScopeMode = 'workspace'; findSimilar(pid); }", photo_ids[0]
    )
    page.locator(".similar-card").click()

    expect(page.locator("#lightboxOverlay")).to_have_class(
        re.compile(r"\bactive\b")
    )
    assert page.evaluate("_lightboxCurrentId") == result_id
    assert page.evaluate("_lightboxPhotoList.map(photo => photo.id)") == [result_id]
    assert page.evaluate("_lbReadOnly") is True
    expect(page.locator("#lightboxDeleteBtn")).to_be_disabled()

    page.evaluate(
        "closeLightbox(); reviewScopeMode = 'cache'; openGroupReview(0, 0)"
    )
    page.wait_for_function("grmState && grmState.seeded === true")
    page.evaluate(
        "pid => { grmSetApplying(true); findSimilar(pid); }", photo_ids[0]
    )
    page.locator(".similar-card").click()

    expect(page.locator("#lightboxOverlay")).to_have_class(
        re.compile(r"\bactive\b")
    )
    assert page.evaluate("_lbReadOnly") is True
    assert page.evaluate("_lightboxPhotoList.map(photo => photo.id)") == [result_id]
    assert page.evaluate("_lbReadOnlyMessage") == (
        "Group Review is applying changes. Wait for it to finish."
    )
    expect(page.locator("#lightboxDeleteBtn")).to_be_disabled()

    native_write_state = page.evaluate(
        """async () => {
          const originalSafeFetch = window.safeFetch;
          const calls = [];
          window.safeFetch = function(url) {
            if (url === '/api/batch/rating' ||
                (url.startsWith('/api/photos/') &&
                 (url.endsWith('/flag') || url.endsWith('/wildlife_excluded')))) {
              calls.push(url);
              return Promise.resolve({});
            }
            return originalSafeFetch.apply(this, arguments);
          };
          try {
            await nativeMenuSetRating(5);
            await nativeMenuSetWildlifeExcluded(true);
            await nativeMenuSetFlag('flagged');
            await Promise.resolve();
            return {calls: calls, flagWritesPending: _lbFlagPendingWrites};
          } finally {
            window.safeFetch = originalSafeFetch;
          }
        }"""
    )
    assert native_write_state == {"calls": [], "flagWritesPending": 0}

    page.evaluate("grmSetApplying(false)")
    assert page.evaluate("_lbReadOnly") is False
    expect(page.locator("#lightboxDeleteBtn")).to_be_enabled()
    expect(page.locator("#lightboxEditPhoto")).to_be_enabled()


def test_lightbox_flag_over_group_review_updates_pending_zones(live_server, page):
    """A flag set from the lightbox above Group Review must route through the
    burst's pending zones, not write the database directly — otherwise the
    burst's Apply overwrites the newer flag with its stale zone snapshot."""
    db = live_server["db"]
    photo_ids = live_server["data"]["photos"][:4]
    _write_grouped_pipeline_cache(live_server, photo_ids)

    page.goto(f"{live_server['url']}/pipeline/review")
    expect(page.locator(".photo-card[data-photo-id]")).to_have_count(4)
    page.evaluate("openGroupReview(0, 0)")
    expect(page.locator("#grmOverlay")).to_have_class(re.compile(r"\bopen\b"))
    burst_ids = photo_ids[:2]

    page.evaluate(
        "pid => openPipelineLightbox(pid)",
        burst_ids[0],
    )
    expect(page.locator("#lightboxOverlay")).to_have_class(
        re.compile(r"\bactive\b")
    )

    # Simulate the lightbox flag action for the visible burst photo. The write
    # must land in the burst's pending zones — not the database — so a later
    # Apply from Group Review reflects it instead of clobbering it.
    landed = page.evaluate(
        "pid => window.setFlagFor(pid, 'rejected')",
        burst_ids[0],
    )
    assert landed is not False

    zones = page.evaluate(
        "() => ({"
        "picks: Array.from(grmState.picks),"
        "rejects: Array.from(grmState.rejects),"
        "touched: Array.from(grmState.touched || []),"
        "})"
    )
    assert zones["rejects"] == [burst_ids[0]]
    assert zones["picks"] == []
    assert burst_ids[0] in zones["touched"]
    assert _flags(db, photo_ids) == ["none"] * 4


def test_lightbox_provisional_group_flag_does_not_poison_confirmed_cache(
    live_server, page
):
    """A lightbox flag staged in Group Review must remain visibly pending
    without mutating pipelineResults or the lightbox's confirmed flag cache."""
    db = live_server["db"]
    photo_ids = live_server["data"]["photos"][:4]
    _write_grouped_pipeline_cache(live_server, photo_ids)

    page.goto(f"{live_server['url']}/pipeline/review")
    expect(page.locator(".photo-card[data-photo-id]")).to_have_count(4)
    page.evaluate("openGroupReview(0, 0)")
    page.wait_for_function("grmState && grmState.seeded === true")
    photo_id = photo_ids[0]
    page.evaluate("pid => openPipelineLightbox(pid)", photo_id)
    expect(page.locator("#lightboxOverlay")).to_have_class(re.compile(r"\bactive\b"))

    page.evaluate("pid => _lbApplyFlag(pid, 'rejected')", photo_id)
    page.wait_for_function("_lbFlagPendingWrites === 0")

    state = page.evaluate(
        "pid => ({"
        "confirmed: _lbConfirmedFlagFor(pid),"
        "cached: pipelineResults.photos.find(p => p.id === pid).flag,"
        "rejected: grmState.rejects.has(pid),"
        "status: document.getElementById('lightboxFlagStatus').textContent,"
        "})",
        photo_id,
    )
    assert state == {
        "confirmed": "none",
        "cached": "none",
        "rejected": True,
        "status": "Rejected",
    }
    assert _flags(db, photo_ids) == ["none"] * 4

    # A later group shortcut supersedes the lightbox choice. Reopening the
    # lightbox must show the current pending zone, not the earlier cached one.
    page.evaluate("closeLightbox(); grmMovePick()")
    page.evaluate("pid => openPipelineLightbox(pid)", photo_id)
    expect(page.locator("#lightboxFlagStatus")).to_have_text("Flagged")
    page.evaluate("closeLightbox(); grmMoveCandidate()")
    page.evaluate("pid => openPipelineLightbox(pid)", photo_id)
    expect(page.locator("#lightboxFlagStatus")).to_have_text("No flag")

    # Discard the group without Apply. The main grid must still reflect the
    # confirmed database/cache value rather than the abandoned pending zone.
    page.evaluate("closeLightbox(); closeGroupReview(); renderResults()")
    expect(
        page.locator(f'.photo-card[data-photo-id="{photo_id}"] .flag-rejected')
    ).to_have_count(0)
    assert _flags(db, photo_ids) == ["none"] * 4


def test_older_persisted_flag_does_not_clear_newer_group_provisional_flag(
    live_server, page
):
    photo_ids = live_server["data"]["photos"][:4]
    _write_grouped_pipeline_cache(live_server, photo_ids)

    page.goto(f"{live_server['url']}/pipeline/review")
    expect(page.locator(".photo-card[data-photo-id]")).to_have_count(4)
    photo_id = photo_ids[0]
    page.evaluate(
        """pid => {
          window.__originalSetFlagFor = window.setFlagFor;
          window.setFlagFor = function() {
            return new Promise(resolve => { window.__resolvePersistedFlag = resolve; });
          };
          openPipelineLightbox(pid);
          _lbApplyFlag(pid, 'flagged');
          closeLightbox();
        }""",
        photo_id,
    )
    page.evaluate("openGroupReview(0, 0)")
    page.wait_for_function("grmState && grmState.seeded === true")
    page.evaluate("grmMoveReject()")
    assert page.evaluate(
        "pid => _lbProvisionalFlags[String(pid)]", photo_id
    ) == "rejected"

    try:
        page.evaluate("window.__resolvePersistedFlag(true)")
        page.wait_for_function("_lbFlagPendingWrites === 0")
        state = page.evaluate(
            "pid => ({"
            "confirmed: _lbConfirmedFlagFor(pid),"
            "provisional: _lbProvisionalFlags[String(pid)],"
            "})",
            photo_id,
        )
        assert state == {
            "confirmed": "flagged",
            "provisional": "rejected",
        }
        page.evaluate("pid => openPipelineLightbox(pid)", photo_id)
        expect(page.locator("#lightboxFlagStatus")).to_have_text("Rejected")
    finally:
        page.evaluate("window.setFlagFor = window.__originalSetFlagFor")


def test_group_apply_waits_for_earlier_direct_flag_write(live_server, page):
    db = live_server["db"]
    photo_ids = live_server["data"]["photos"][:4]
    _write_grouped_pipeline_cache(live_server, photo_ids)

    page.goto(f"{live_server['url']}/pipeline/review")
    expect(page.locator(".photo-card[data-photo-id]")).to_have_count(4)
    photo_id = photo_ids[0]
    page.evaluate(
        """pid => {
          window.__originalSafeFetchForFlagOrdering = window.safeFetch;
          window.safeFetch = function(url, options) {
            if (url === '/api/photos/' + pid + '/flag') {
              const args = arguments;
              return new Promise((resolve, reject) => {
                window.__releaseEarlierFlagWrite = () => {
                  window.__originalSafeFetchForFlagOrdering
                    .apply(window, args).then(resolve, reject);
                };
              });
            }
            return window.__originalSafeFetchForFlagOrdering.apply(this, arguments);
          };
          window.__earlierFlagWrite = setPipelineReviewFlag(pid, 'flagged');
        }""",
        photo_id,
    )
    page.evaluate("openGroupReview(0, 0)")
    page.wait_for_function("grmState && grmState.seeded === true")
    page.evaluate(
        """() => {
          grmMoveReject();
          window.__groupApplySettled = false;
          window.__groupApplyPromise = grmApply().then(
            () => { window.__groupApplySettled = true; },
            error => {
              window.__groupApplyError = String(error);
              window.__groupApplySettled = true;
            }
          );
        }"""
    )
    page.wait_for_timeout(100)
    assert page.evaluate("window.__groupApplySettled") is False
    assert _flags(db, photo_ids) == ["none"] * 4

    try:
        page.evaluate("window.__releaseEarlierFlagWrite()")
        page.wait_for_function("window.__groupApplySettled === true")
        assert page.evaluate("window.__groupApplyError || null") is None
        assert _flags(db, photo_ids) == ["rejected", "none", "none", "none"]
        assert page.evaluate(
            "pid => !pipelineReviewDirectFlagWritesByPhoto[String(pid)]", photo_id
        ) is True
    finally:
        page.evaluate(
            "() => { window.safeFetch = window.__originalSafeFetchForFlagOrdering; }"
        )


def test_waiting_group_apply_aborts_after_another_group_opens(live_server, page):
    db = live_server["db"]
    photo_ids = live_server["data"]["photos"][:4]
    _write_grouped_pipeline_cache(live_server, photo_ids)

    page.goto(f"{live_server['url']}/pipeline/review")
    expect(page.locator(".photo-card[data-photo-id]")).to_have_count(4)
    photo_id = photo_ids[0]
    page.evaluate(
        """pid => {
          window.__originalSafeFetchForSessionGuard = window.safeFetch;
          window.__groupApplyCalls = 0;
          window.safeFetch = function(url, options) {
            if (url === '/api/photos/' + pid + '/flag') {
              const args = arguments;
              return new Promise((resolve, reject) => {
                window.__releaseSessionGuardFlag = () => {
                  window.__originalSafeFetchForSessionGuard
                    .apply(window, args).then(resolve, reject);
                };
              });
            }
            if (url === '/api/pipeline/group/apply') window.__groupApplyCalls += 1;
            return window.__originalSafeFetchForSessionGuard.apply(this, arguments);
          };
          window.__sessionGuardFlagWrite = setPipelineReviewFlag(pid, 'flagged');
        }""",
        photo_id,
    )
    page.evaluate("openGroupReview(0, 0)")
    page.wait_for_function("grmState && grmState.seeded === true")
    original_session = page.evaluate("grmState.sessionId")
    page.evaluate(
        """() => {
          grmMoveReject();
          window.__staleApplySettled = false;
          window.__staleApplyPromise = grmApply().then(
            () => { window.__staleApplySettled = true; },
            error => {
              window.__staleApplyError = String(error);
              window.__staleApplySettled = true;
            }
          );
          closeGroupReview();
          openGroupReview(0, 1);
        }"""
    )
    page.wait_for_function(
        "session => grmState.sessionId !== session && grmState.seeded === true",
        arg=original_session,
    )
    new_group_ids = page.evaluate("grmState.items.map(photo => photo.id)")

    try:
        page.evaluate("window.__releaseSessionGuardFlag()")
        page.wait_for_function("window.__staleApplySettled === true")
        assert page.evaluate("window.__staleApplyError || null") is None
        assert page.evaluate("window.__groupApplyCalls") == 0
        assert page.evaluate("grmState.items.map(photo => photo.id)") == new_group_ids
        assert page.evaluate("grmState.applying") is False
        assert _flags(db, photo_ids) == ["flagged", "none", "none", "none"]
    finally:
        page.evaluate(
            "() => { window.safeFetch = window.__originalSafeFetchForSessionGuard; }"
        )


def test_group_apply_freezes_controls_and_aborts_after_later_session_change(
    live_server, page
):
    db = live_server["db"]
    photo_ids = live_server["data"]["photos"][:4]
    _write_grouped_pipeline_cache(live_server, photo_ids)

    page.goto(f"{live_server['url']}/pipeline/review")
    expect(page.locator(".photo-card[data-photo-id]")).to_have_count(4)
    page.evaluate("openGroupReview(0, 0)")
    page.wait_for_function("grmState && grmState.seeded === true")
    original_session = page.evaluate("grmState.sessionId")
    page.evaluate(
        """() => {
          window.__originalSafeFetchForApplyLifecycle = window.safeFetch;
          window.__lifecycleSaveCacheCalls = 0;
          window.safeFetch = function(url, options) {
            if (url === '/api/pipeline/group/apply') {
              const args = arguments;
              return new Promise((resolve, reject) => {
                window.__releaseLifecycleApply = () => {
                  window.__originalSafeFetchForApplyLifecycle
                    .apply(window, args).then(resolve, reject);
                };
              });
            }
            if (url === '/api/pipeline/save-cache') {
              window.__lifecycleSaveCacheCalls += 1;
            }
            return window.__originalSafeFetchForApplyLifecycle.apply(this, arguments);
          };
          grmMoveReject();
          openPipelineLightbox(grmState.selected);
          window.__lifecycleApplySettled = false;
          window.__lifecycleApplyPromise = grmApply().then(
            () => { window.__lifecycleApplySettled = true; },
            error => {
              window.__lifecycleApplyError = String(error);
              window.__lifecycleApplySettled = true;
            }
          );
        }"""
    )
    page.wait_for_function("typeof window.__releaseLifecycleApply === 'function'")

    frozen_state = page.evaluate(
        """() => {
          const selected = grmState.selected;
          const rejectedBefore = grmState.rejects.has(selected);
          return {
            applying: grmState.applying,
            inert: document.getElementById('grmOverlay').inert,
            closeResult: closeGroupReview(),
            moveResult: grmMovePick(),
            stillRejected: rejectedBefore && grmState.rejects.has(selected),
            stillOpen: document.getElementById('grmOverlay').classList.contains('open'),
          };
        }"""
    )
    assert frozen_state == {
        "applying": True,
        "inert": True,
        "closeResult": False,
        "moveResult": False,
        "stillRejected": True,
        "stillOpen": True,
    }

    selected_id = page.evaluate(
        """() => {
          const selected = grmState.selected;
          _lbApplyFlag(selected, 'flagged');
          return selected;
        }"""
    )
    page.wait_for_function("_lbFlagPendingWrites === 0")
    late_lightbox_state = page.evaluate(
        """pid => ({
          picked: grmState.picks.has(pid),
          rejected: grmState.rejects.has(pid),
          directWrite: !!pipelineReviewDirectFlagWritesByPhoto[String(pid)],
        })""",
        selected_id,
    )
    assert late_lightbox_state == {
        "picked": False,
        "rejected": True,
        "directWrite": False,
    }
    assert _flags(db, photo_ids) == ["none"] * 4

    reopen_state = page.evaluate(
        """pid => {
          closeLightbox();
          return {
            result: openPipelineLightbox(pid),
            lightboxOpen: document.getElementById('lightboxOverlay')
              .classList.contains('active'),
          };
        }""",
        selected_id,
    )
    assert reopen_state == {"result": False, "lightboxOpen": False}

    page.evaluate("openGroupReview(0, 1)")
    page.wait_for_function(
        "session => grmState.sessionId !== session && grmState.seeded === true",
        arg=original_session,
    )
    replacement_state = page.evaluate(
        """() => ({
          sessionId: grmState.sessionId,
          itemIds: grmState.items.map(photo => photo.id),
          picks: Array.from(grmState.picks),
          rejects: Array.from(grmState.rejects),
          applying: grmState.applying,
          inert: document.getElementById('grmOverlay').inert,
        })"""
    )

    try:
        page.evaluate("window.__releaseLifecycleApply()")
        page.wait_for_function("window.__lifecycleApplySettled === true")
        assert page.evaluate("window.__lifecycleApplyError || null") is None
        assert page.evaluate("window.__lifecycleSaveCacheCalls") == 0
        assert page.evaluate(
            """() => ({
              sessionId: grmState.sessionId,
              itemIds: grmState.items.map(photo => photo.id),
              picks: Array.from(grmState.picks),
              rejects: Array.from(grmState.rejects),
              applying: grmState.applying,
              inert: document.getElementById('grmOverlay').inert,
            })"""
        ) == replacement_state
        assert _flags(db, photo_ids) == ["rejected", "none", "none", "none"]
    finally:
        page.evaluate(
            "() => { window.safeFetch = window.__originalSafeFetchForApplyLifecycle; }"
        )


def test_read_only_scope_lightbox_flag_does_not_mutate_cache(live_server, page):
    db = live_server["db"]
    photo_ids = live_server["data"]["photos"][:4]
    _write_grouped_pipeline_cache(live_server, photo_ids)

    page.goto(f"{live_server['url']}/pipeline/review")
    expect(page.locator(".photo-card[data-photo-id]")).to_have_count(4)
    photo_id = photo_ids[0]
    page.evaluate("reviewScopeMode = 'workspace'")
    page.evaluate("pid => openPipelineLightbox(pid)", photo_id)
    page.evaluate("pid => _lbApplyFlag(pid, 'rejected')", photo_id)
    page.wait_for_function("_lbFlagPendingWrites === 0")

    state = page.evaluate(
        "pid => ({"
        "confirmed: _lbConfirmedFlagFor(pid),"
        "cached: pipelineResults.photos.find(p => p.id === pid).flag,"
        "status: document.getElementById('lightboxFlagStatus').textContent,"
        "})",
        photo_id,
    )
    assert state == {"confirmed": "none", "cached": "none", "status": "No flag"}
    assert _flags(db, photo_ids) == ["none"] * 4


def test_read_only_scope_rating_does_not_mutate_cache_or_database(live_server, page):
    db = live_server["db"]
    photo_ids = live_server["data"]["photos"][:4]
    _write_grouped_pipeline_cache(live_server, photo_ids)

    page.goto(f"{live_server['url']}/pipeline/review")
    expect(page.locator(".photo-card[data-photo-id]")).to_have_count(4)
    photo_id = photo_ids[0]
    before_cache = page.evaluate(
        "pid => pipelineResults.photos.find(photo => photo.id === pid).rating",
        photo_id,
    )
    before_db = db.conn.execute(
        "SELECT rating FROM photos WHERE id = ?", (photo_id,)
    ).fetchone()["rating"]
    attempted_rating = next(
        value for value in range(6) if value not in {before_cache, before_db}
    )
    result = page.evaluate(
        "args => { reviewScopeMode = 'workspace'; "
        "return setRatingFor(args.photoId, args.rating); }",
        {"photoId": photo_id, "rating": attempted_rating},
    )
    assert result is False
    assert page.evaluate(
        "pid => pipelineResults.photos.find(photo => photo.id === pid).rating",
        photo_id,
    ) == before_cache
    rating = db.conn.execute(
        "SELECT rating FROM photos WHERE id = ?", (photo_id,)
    ).fetchone()["rating"]
    assert rating == before_db


def test_read_only_scope_blocks_collection_and_keyword_writes(live_server, page):
    db = live_server["db"]
    photo_ids = live_server["data"]["photos"][:4]
    _write_grouped_pipeline_cache(live_server, photo_ids)
    collection_id = db.add_collection(
        "Read-only target",
        json.dumps([{"field": "photo_ids", "value": []}]),
    )

    page.goto(f"{live_server['url']}/pipeline/review")
    expect(page.locator(".photo-card[data-photo-id]")).to_have_count(4)
    photo_id = photo_ids[0]

    # Stage both dialogs in Latest, then switch scope before confirming. The
    # confirmation-time guards must prevent either POST even though the forms
    # already contain valid targets.
    page.evaluate("pid => addToCollection([pid])", photo_id)
    expect(page.locator("#pipelineCollectionModal")).to_have_class(
        re.compile(r"\bopen\b")
    )
    page.evaluate("cid => pickPipelineCollection(cid)", collection_id)
    page.evaluate("pid => batchAddKeyword([pid])", photo_id)
    page.locator("#pipelineKeywordInput").fill("must-not-write")
    results = page.evaluate(
        "async () => { reviewScopeMode = 'workspace'; return Promise.all(["
        "confirmPipelineCollection(), confirmPipelineKeyword()]); }"
    )
    assert results == [False, False]
    assert db.get_collection_photos(collection_id, per_page=100) == []
    assert "must-not-write" not in {
        keyword["name"] for keyword in db.get_photo_keywords(photo_id)
    }

    # The action entry points are guarded as well, so native menu calls and
    # other direct invocations cannot open writable dialogs in a scoped view.
    page.evaluate("hidePipelineKeywordModal(true); hidePipelineCollectionModal(true)")
    page.locator(f'.photo-card[data-photo-id="{photo_id}"]').click(button="right")
    menu = page.locator(".vireo-ctx-menu")
    for label in (
        "Add to Collection",
        "Add Keyword",
        "Edit Photo",
        "Open in Editor",
        "Develop in darktable",
    ):
        item = menu.locator(".vireo-ctx-item", has_text=label)
        expect(item).to_have_class(re.compile(r"\bvireo-ctx-disabled\b"))
        expect(item).to_have_attribute("title", "Switch to Latest review to make changes")
    assert "Add to Highlights" not in menu.inner_text()
    assert "Set Representative" not in menu.inner_text()
    page.keyboard.press("Escape")

    original_url = page.url
    blocked = page.evaluate(
        """async pid => {
          window._vireoNativeMenuPhotoIdsOverride = [pid];
          const collection = await addToCollection([pid]);
          const collectionOverride = window._vireoNativeMenuPhotoIdsOverride;
          window._vireoNativeMenuPhotoIdsOverride = [pid];
          const keyword = batchAddKeyword([pid]);
          const editor = openPipelinePhotoEditor(pid);
          return {
            collection: collection,
            keyword: keyword,
            editor: editor,
            collectionOverride: collectionOverride,
            keywordOverride: window._vireoNativeMenuPhotoIdsOverride,
            collectionOpen: document.getElementById('pipelineCollectionModal')
              .classList.contains('open'),
            keywordOpen: document.getElementById('pipelineKeywordModal')
              .classList.contains('open'),
          };
        }""",
        photo_id,
    )
    assert blocked == {
        "collection": False,
        "keyword": False,
        "editor": False,
        "collectionOverride": None,
        "keywordOverride": None,
        "collectionOpen": False,
        "keywordOpen": False,
    }
    assert page.url == original_url

    external_edits = page.evaluate(
        """async pid => {
          const originalSafeFetch = window.safeFetch;
          const calls = [];
          window.safeFetch = function(url) {
            calls.push(url);
            return Promise.resolve({available: false, opened: 1});
          };
          try {
            return {
              editor: await openInEditor([pid]),
              develop: await developPhotos([pid]),
              calls: calls,
            };
          } finally {
            window.safeFetch = originalSafeFetch;
          }
        }""",
        photo_id,
    )
    assert external_edits == {"editor": False, "develop": False, "calls": []}


def test_read_only_scope_disables_lightbox_and_native_mutations(live_server, page):
    db = live_server["db"]
    photo_ids = live_server["data"]["photos"][:4]
    _write_grouped_pipeline_cache(live_server, photo_ids)

    page.goto(f"{live_server['url']}/pipeline/review")
    expect(page.locator(".photo-card[data-photo-id]")).to_have_count(4)
    photo_id = photo_ids[0]
    before_wildlife = db.conn.execute(
        "SELECT wildlife_excluded FROM photos WHERE id = ?", (photo_id,)
    ).fetchone()["wildlife_excluded"]
    page.evaluate(
        "pid => { reviewScopeMode = 'workspace'; openPipelineLightbox(pid); }",
        photo_id,
    )
    expect(page.locator("#lightboxOverlay")).to_have_class(
        re.compile(r"\bactive\b")
    )

    for selector in (
        "#lightboxInat",
        "#lightboxAdjustBtn",
        "#lightboxDeleteBtn",
        "#lightboxEditPhoto",
    ):
        expect(page.locator(selector)).to_be_disabled()

    state = page.evaluate(
        """async () => {
          const adjustment = document.getElementById('lbAdjExposure');
          return {
            readOnly: _lbReadOnly,
            deleteResult: lightboxDelete(),
            wildlifeResult: await lightboxToggleWildlifeExcluded(),
            nativeWildlifeResult: await nativeMenuSetWildlifeExcluded(true),
            inatResult: await submitToInat(_lightboxCurrentId),
            inatSubmitResult: await inatDoSubmit(),
            adjustmentResult: onLightboxAdjustmentInput(adjustment),
            cropResult: await openCropEditor(),
            editResult: document.getElementById('lightboxEditPhoto').onclick(),
            deleteOpen: document.getElementById('deleteModal').classList.contains('open'),
            inatOpen: document.getElementById('inatModal').classList.contains('open'),
            adjustmentTimer: _lbAdjustSaveTimer,
          };
        }"""
    )
    assert state == {
        "readOnly": True,
        "deleteResult": False,
        "wildlifeResult": False,
        "nativeWildlifeResult": False,
        "inatResult": False,
        "inatSubmitResult": False,
        "adjustmentResult": False,
        "cropResult": False,
        "editResult": False,
        "deleteOpen": False,
        "inatOpen": False,
        "adjustmentTimer": None,
    }
    after_wildlife = db.conn.execute(
        "SELECT wildlife_excluded FROM photos WHERE id = ?", (photo_id,)
    ).fetchone()["wildlife_excluded"]
    assert after_wildlife == before_wildlife

    page.locator("#lightboxImg").dispatch_event(
        "contextmenu", {"button": 2, "clientX": 400, "clientY": 300}
    )
    menu = page.locator(".vireo-ctx-menu")
    expect(menu.locator(".vireo-ctx-chip.vireo-ctx-disabled")).to_have_count(9)
    wildlife_item = menu.locator(
        ".vireo-ctx-item", has_text="Wildlife classification"
    )
    expect(wildlife_item).to_have_class(re.compile(r"\bvireo-ctx-disabled\b"))
    expect(menu.locator(".vireo-ctx-item", has_text="Open in Editor")).to_have_class(
        re.compile(r"\bvireo-ctx-disabled\b")
    )


def test_lightbox_deletion_updates_process_and_open_group_state(live_server, page):
    photo_ids = live_server["data"]["photos"][:4]
    _write_grouped_pipeline_cache(live_server, photo_ids)

    page.goto(f"{live_server['url']}/pipeline/review")
    expect(page.locator(".photo-card[data-photo-id]")).to_have_count(4)
    page.evaluate("openGroupReview(0, 0)")
    page.wait_for_function("grmState && grmState.seeded === true")
    deleted_id = page.evaluate("grmState.selected")
    page.evaluate("grmMoveReject()")
    assert page.evaluate("grmHasPendingUserEdits()") is True

    state = page.evaluate(
        """photoId => {
          document.dispatchEvent(new CustomEvent('lightbox:photodeleted', {
            detail: {photoId: photoId, result: {deleted: 1}},
          }));
          const encounterIds = pipelineResults.encounters.flatMap(
            encounter => encounter.photo_ids || []
          );
          const burstIds = pipelineResults.encounters.flatMap(
            encounter => (encounter.bursts || []).flatMap(
              burst => burst.photo_ids || burst || []
            )
          );
          return {
            photoIds: pipelineResults.photos.map(photo => photo.id),
            encounterIds: encounterIds,
            burstIds: burstIds,
            groupIds: grmState.items.map(photo => photo.id),
            rejectedIds: Array.from(grmState.rejects),
            selectedIds: Array.from(grmState.selectedIds),
            selected: grmState.selected,
            rejectDiff: grmComputeDiff().rejectNew,
            loupeSrc: document.getElementById('grmLoupePhoto').getAttribute('src'),
            expectedLoupeSrc: grmState.selected ? grmPhotoUrl(grmState.selected) : null,
          };
        }""",
        deleted_id,
    )

    for key in (
        "photoIds",
        "encounterIds",
        "burstIds",
        "groupIds",
        "rejectedIds",
        "selectedIds",
    ):
        assert deleted_id not in state[key]
    assert state["selected"] != deleted_id
    assert state["rejectDiff"] == 0
    assert state["loupeSrc"] == state["expectedLoupeSrc"]
    expect(page.locator(f'.photo-card[data-photo-id="{deleted_id}"]')).to_have_count(0)
    expect(page.locator(f'.grm-card[data-photo-id="{deleted_id}"]')).to_have_count(0)


def test_lightbox_deletion_skips_staged_removed_selection_fallback(
    live_server, page
):
    photo_ids = live_server["data"]["photos"][:4]
    _write_grouped_pipeline_cache(live_server, photo_ids)

    page.goto(f"{live_server['url']}/pipeline/review")
    expect(page.locator(".photo-card[data-photo-id]")).to_have_count(4)
    page.evaluate("openGroupReview(0, 0)")
    page.wait_for_function("grmState && grmState.seeded === true")
    deleted_id = page.evaluate("grmState.selected")
    page.evaluate(
        """deletedId => {
          const survivor = grmState.items.find(photo => photo.id !== deletedId);
          grmState.removed.add(survivor.id);
          grmSyncZoneCards();
        }""",
        deleted_id,
    )

    state = page.evaluate(
        """photoId => {
          document.dispatchEvent(new CustomEvent('lightbox:photodeleted', {
            detail: {photoId: photoId, result: {deleted: 1}},
          }));
          return {
            selected: grmState.selected,
            selectedIds: Array.from(grmState.selectedIds),
            loupeSrc: document.getElementById('grmLoupePhoto').getAttribute('src'),
            loupeInfo: document.getElementById('grmLoupeInfo').textContent,
          };
        }""",
        deleted_id,
    )
    assert state == {
        "selected": None,
        "selectedIds": [],
        "loupeSrc": None,
        "loupeInfo": (
            "Select a photo to preview. Hover to compare sharpness across all frames."
        ),
    }


def test_lightbox_deletion_remaps_open_group_after_pruning_earlier_units(
    live_server, page
):
    photo_ids = live_server["data"]["photos"][:4]
    _write_grouped_pipeline_cache(live_server, photo_ids)

    page.goto(f"{live_server['url']}/pipeline/review")
    expect(page.locator(".photo-card[data-photo-id]")).to_have_count(4)
    page.evaluate(
        """ids => {
          pipelineResults.encounters = [{
            photo_ids: [ids[0]],
            photo_count: 1,
            burst_count: 1,
            species: [],
            species_predictions: [],
            species_confirmed: false,
            confirmed_species: null,
            bursts: [{
              photo_ids: [ids[0]],
              species_predictions: [],
              species_override: null,
            }],
          }, {
            photo_ids: [ids[1], ids[2], ids[3]],
            photo_count: 3,
            burst_count: 2,
            species: [],
            species_predictions: [],
            species_confirmed: false,
            confirmed_species: null,
            bursts: [{
              photo_ids: [ids[1]],
              species_predictions: [],
              species_override: null,
            }, {
              photo_ids: [ids[2], ids[3]],
              species_predictions: [],
              species_override: null,
            }],
          }];
          renderResults();
          openGroupReview(1, 1);
        }""",
        photo_ids,
    )
    page.wait_for_function("grmState && grmState.seeded === true")
    group_ids = photo_ids[2:4]

    first_shift = page.evaluate(
        """photoId => {
          document.dispatchEvent(new CustomEvent('lightbox:photodeleted', {
            detail: {photoId: photoId, result: {deleted: 1}},
          }));
          const burst = pipelineResults.encounters[grmState.encIdx]
            .bursts[grmState.burstIdx];
          return {
            encIdx: grmState.encIdx,
            burstIdx: grmState.burstIdx,
            groupIds: grmState.items.map(photo => photo.id),
            burstIds: (burst.photo_ids || burst).slice(),
          };
        }""",
        photo_ids[0],
    )
    assert first_shift == {
        "encIdx": 0,
        "burstIdx": 1,
        "groupIds": group_ids,
        "burstIds": group_ids,
    }

    second_shift = page.evaluate(
        """photoId => {
          document.dispatchEvent(new CustomEvent('lightbox:photodeleted', {
            detail: {photoId: photoId, result: {deleted: 1}},
          }));
          const burst = pipelineResults.encounters[grmState.encIdx]
            .bursts[grmState.burstIdx];
          return {
            encIdx: grmState.encIdx,
            burstIdx: grmState.burstIdx,
            groupIds: grmState.items.map(photo => photo.id),
            burstIds: (burst.photo_ids || burst).slice(),
            open: document.getElementById('grmOverlay').classList.contains('open'),
          };
        }""",
        photo_ids[1],
    )
    assert second_shift == {
        "encIdx": 0,
        "burstIdx": 0,
        "groupIds": group_ids,
        "burstIds": group_ids,
        "open": True,
    }


def test_lightbox_deletion_prunes_empty_encounter_from_summary(live_server, page):
    photo_ids = live_server["data"]["photos"][:4]
    _write_grouped_pipeline_cache(live_server, photo_ids)

    page.goto(f"{live_server['url']}/pipeline/review")
    expect(page.locator(".photo-card[data-photo-id]")).to_have_count(4)
    state = page.evaluate(
        """photoId => {
          const photo = pipelineResults.photos.find(item => item.id === photoId);
          pipelineResults.photos = [photo];
          pipelineResults.encounters = [{
            photo_ids: [photoId],
            photo_count: 1,
            burst_count: 1,
            bursts: [{photo_ids: [photoId]}],
          }];
          pipelineResults.summary = {total_photos: 1, encounter_count: 1, burst_count: 1};
          pipelineReviewContextPhotoIds = [photoId];
          window._vireoNativeMenuPhotoIdsOverride = [photoId];
          inspectPhotoId = photoId;
          document.getElementById('inspectOverlay').classList.add('open');
          document.dispatchEvent(new CustomEvent('lightbox:photodeleted', {
            detail: {photoId: photoId, result: {deleted: 1}},
          }));
          return {
            photos: pipelineResults.photos.length,
            encounters: pipelineResults.encounters.length,
            totalPhotos: pipelineResults.summary.total_photos,
            encounterCount: pipelineResults.summary.encounter_count,
            burstCount: pipelineResults.summary.burst_count,
            contextIds: pipelineReviewContextPhotoIds.slice(),
            nativeOverride: window._vireoNativeMenuPhotoIdsOverride,
            inspectPhotoId: inspectPhotoId,
            inspectOpen: document.getElementById('inspectOverlay').classList.contains('open'),
            activeSelection: getActiveSelection(),
          };
        }""",
        photo_ids[0],
    )
    assert state == {
        "photos": 0,
        "encounters": 0,
        "totalPhotos": 0,
        "encounterCount": 0,
        "burstCount": 0,
        "contextIds": [],
        "nativeOverride": None,
        "inspectPhotoId": None,
        "inspectOpen": False,
        "activeSelection": [],
    }


def test_tauri_disables_navigation_when_group_review_is_dirty(live_server, page):
    photo_ids = live_server["data"]["photos"][:4]
    _write_grouped_pipeline_cache(live_server, photo_ids)

    page.goto(f"{live_server['url']}/pipeline/review")
    expect(page.locator(".photo-card[data-photo-id]")).to_have_count(4)
    page.evaluate("openGroupReview(0, 0)")
    page.wait_for_function("grmState && grmState.seeded === true")
    page.evaluate("window.__TAURI_INTERNALS__ = {}; grmMoveReject()")

    card = page.locator("#grmOverlay .grm-card[data-photo-id]").first
    card.click(button="right")
    menu = page.locator(".vireo-ctx-menu")
    for label in ("Open in Browse", "Edit Photo"):
        item = menu.locator(".vireo-ctx-item", has_text=label)
        expect(item).to_have_class(re.compile(r"\bvireo-ctx-disabled\b"))
        expect(item).to_have_attribute(
            "title", "Apply or close Group Review before leaving this page"
        )

    page.evaluate("pid => openPipelineLightbox(pid)", photo_ids[0])
    page.locator("#lightboxImg").dispatch_event(
        "contextmenu", {"button": 2, "clientX": 400, "clientY": 300}
    )
    lightbox_browse = page.locator(".vireo-ctx-item", has_text="Open in Browse")
    expect(lightbox_browse).to_have_class(re.compile(r"\bvireo-ctx-disabled\b"))
    expect(lightbox_browse).to_have_attribute(
        "title", "Apply or close Group Review before leaving this page"
    )
    edit_button = page.locator("#lightboxEditPhoto")
    expect(edit_button).to_be_disabled()
    expect(edit_button).to_have_attribute(
        "title", "Apply or close Group Review before leaving this page"
    )
    original_url = page.url
    assert page.evaluate(
        "document.getElementById('lightboxEditPhoto').onclick()"
    ) is False
    assert page.url == original_url

    native_state = page.evaluate(
        """() => {
          const originalRoute = window.nativeMenuRoute;
          window.__nativeBrowseRoute = null;
          window.nativeMenuRoute = path => { window.__nativeBrowseRoute = path; };
          try {
            return {
              result: nativeMenuOpenBrowse(),
              route: window.__nativeBrowseRoute,
            };
          } finally {
            window.nativeMenuRoute = originalRoute;
          }
        }"""
    )
    assert native_state == {"result": False, "route": None}


def test_tauri_edit_photo_allows_untouched_suggested_species(live_server, page):
    """A prefilled prediction is an Apply suggestion, not a user edit."""
    photo_ids = live_server["data"]["photos"][:4]
    _write_grouped_pipeline_cache(live_server, photo_ids)

    page.goto(f"{live_server['url']}/pipeline/review")
    expect(page.locator(".photo-card[data-photo-id]")).to_have_count(4)
    page.evaluate(
        "pipelineResults.encounters[0].species = ['American Robin']; "
        "openGroupReview(0, 0)"
    )
    page.wait_for_function("grmState && grmState.seeded === true")
    assert page.locator("#grmSpecies").input_value() == "American Robin"
    assert page.evaluate("grmState.speciesFieldTouched") is False
    assert page.evaluate("grmHasPendingUserEdits()") is False

    page.evaluate("window.__TAURI_INTERNALS__ = {}")
    card = page.locator("#grmOverlay .grm-card[data-photo-id]").first
    photo_id = card.get_attribute("data-photo-id")
    card.click(button="right")
    edit_item = page.locator(".vireo-ctx-item", has_text="Edit Photo")
    expect(edit_item).not_to_have_class(re.compile(r"\bvireo-ctx-disabled\b"))
    edit_item.click()

    page.wait_for_url(f"**/edit/{photo_id}")


def test_popup_blocked_browse_preserves_dirty_group_review(live_server, page):
    photo_ids = live_server["data"]["photos"][:4]
    _write_grouped_pipeline_cache(live_server, photo_ids)

    page.goto(f"{live_server['url']}/pipeline/review")
    expect(page.locator(".photo-card[data-photo-id]")).to_have_count(4)
    page.evaluate("openGroupReview(0, 0)")
    page.wait_for_function("grmState && grmState.seeded === true")
    page.evaluate("grmMoveReject()")
    original_url = page.url

    result = page.evaluate(
        "pid => { const originalOpen = window.open; window.open = () => null; "
        "try { return window.openInBrowse(pid); } finally { window.open = originalOpen; } }",
        photo_ids[0],
    )

    assert result is False
    assert page.url == original_url
    assert page.evaluate("grmHasPendingUserEdits()") is True
    expect(page.locator("#grmOverlay")).to_have_class(re.compile(r"\bopen\b"))


def test_tauri_browse_rechecks_dirty_state_after_menu_opens(live_server, page):
    photo_ids = live_server["data"]["photos"][:4]
    _write_grouped_pipeline_cache(live_server, photo_ids)

    page.goto(f"{live_server['url']}/pipeline/review")
    expect(page.locator(".photo-card[data-photo-id]")).to_have_count(4)
    page.evaluate("openGroupReview(0, 0)")
    page.wait_for_function("grmState && grmState.seeded === true")
    original_url = page.url
    state = page.evaluate(
        """() => {
          window.__TAURI_INTERNALS__ = {};
          const photoId = grmState.selected;
          const browseItem = buildPipelinePhotoContextMenu(
            [photoId], photoId, true
          ).find(item => item.label === 'Open in Browse');
          const editItem = buildPipelinePhotoContextMenu(
            [photoId], photoId, true
          ).find(item => item.label === 'Edit Photo');
          const initiallyDisabled = {
            browse: !!browseItem.disabled,
            edit: !!editItem.disabled,
          };
          grmMoveReject();
          browseItem.onClick();
          editItem.onClick();
          return {
            initiallyDisabled: initiallyDisabled,
            dirty: grmHasPendingUserEdits(),
          };
        }"""
    )
    assert state == {
        "initiallyDisabled": {"browse": False, "edit": False},
        "dirty": True,
    }
    assert page.url == original_url
    expect(page.locator("#grmOverlay")).to_have_class(re.compile(r"\bopen\b"))


def test_tauri_treats_unseeded_group_touches_as_dirty(live_server, page):
    photo_ids = live_server["data"]["photos"][:4]
    _write_grouped_pipeline_cache(live_server, photo_ids)

    page.goto(f"{live_server['url']}/pipeline/review")
    expect(page.locator(".photo-card[data-photo-id]")).to_have_count(4)
    # Model the loading/error window before the live DB snapshot has seeded.
    page.evaluate(
        "window.__TAURI_INTERNALS__ = {}; openGroupReview(0, 0); "
        "grmState.seeded = false; grmMoveReject()"
    )
    assert page.evaluate("grmState.touched.size") == 1

    card = page.locator("#grmOverlay .grm-card[data-photo-id]").first
    card.click(button="right")
    menu = page.locator(".vireo-ctx-menu")
    for label in ("Open in Browse", "Edit Photo"):
        expect(menu.locator(".vireo-ctx-item", has_text=label)).to_have_class(
            re.compile(r"\bvireo-ctx-disabled\b")
        )


def test_apply_time_lightbox_read_only_refreshes_over_full_apply_lifecycle(
    live_server, page
):
    """A Similar-result lightbox opened while /api/pipeline/group/apply is
    still in flight snapshots _lbReadOnly=true. When the real request
    settles and Group Review closes, the lightbox must re-enable — the
    apply-time refresh hook has to survive the entire grmApply cycle, not
    just a synthetic grmSetApplying(false) call."""
    photo_ids = live_server["data"]["photos"][:4]
    _write_grouped_pipeline_cache(live_server, photo_ids)
    result_id = photo_ids[1]
    page.route(
        "**/api/photos/*/similar?limit=40",
        lambda route: route.fulfill(
            json={
                "total_compared": 1,
                "similar": [
                    {
                        "similarity": 0.91,
                        "photo": {"id": result_id, "filename": "similar.jpg"},
                    }
                ],
            }
        ),
    )

    page.goto(f"{live_server['url']}/pipeline/review")
    expect(page.locator(".photo-card[data-photo-id]")).to_have_count(4)
    page.evaluate("openGroupReview(0, 0)")
    page.wait_for_function("grmState && grmState.seeded === true")
    page.evaluate(
        """() => {
          window.__originalSafeFetchForReadOnlyLifecycle = window.safeFetch;
          window.safeFetch = function(url) {
            if (url === '/api/pipeline/group/apply') {
              const args = arguments;
              return new Promise((resolve, reject) => {
                window.__releaseReadOnlyLifecycleApply = () => {
                  window.__originalSafeFetchForReadOnlyLifecycle
                    .apply(window, args).then(resolve, reject);
                };
              });
            }
            return window.__originalSafeFetchForReadOnlyLifecycle
              .apply(this, arguments);
          };
          grmMoveReject();
          window.__readOnlyLifecycleSettled = false;
          window.__readOnlyLifecycleApplyPromise = grmApply().then(
            () => { window.__readOnlyLifecycleSettled = true; },
            error => {
              window.__readOnlyLifecycleApplyError = String(error);
              window.__readOnlyLifecycleSettled = true;
            }
          );
        }"""
    )
    page.wait_for_function(
        "typeof window.__releaseReadOnlyLifecycleApply === 'function'"
    )
    assert page.evaluate("grmState.applying") is True

    page.evaluate("pid => findSimilar(pid)", photo_ids[0])
    page.locator(".similar-card").click()
    expect(page.locator("#lightboxOverlay")).to_have_class(
        re.compile(r"\bactive\b")
    )
    assert page.evaluate("_lightboxCurrentId") == result_id
    assert page.evaluate("_lbReadOnly") is True
    assert page.evaluate("_lbReadOnlyMessage") == (
        "Group Review is applying changes. Wait for it to finish."
    )
    expect(page.locator("#lightboxDeleteBtn")).to_be_disabled()

    try:
        page.evaluate("window.__releaseReadOnlyLifecycleApply()")
        page.wait_for_function("window.__readOnlyLifecycleSettled === true")
        assert page.evaluate("window.__readOnlyLifecycleApplyError || null") is None
        # Apply finished and closeGroupReview() ran; the already-open lightbox
        # must have refreshed its read-only state so arrow-navigation doesn't
        # keep carrying the apply-time snapshot forward.
        assert page.evaluate("_lbReadOnly") is False
        expect(page.locator("#lightboxDeleteBtn")).to_be_enabled()
        expect(page.locator("#lightboxAdjustBtn")).to_be_enabled()
    finally:
        page.evaluate(
            "() => { window.safeFetch = window.__originalSafeFetchForReadOnlyLifecycle; }"
        )


def test_native_rating_and_wildlife_writes_rejected_through_held_apply(
    live_server, page
):
    """Native Photo commands route through setRatingFor/setWildlifeExcludedFor,
    so during a fully held /api/pipeline/group/apply neither may reach the
    server or mutate the local cache or database. Once the real request
    settles they must both work again — no sticky lock."""
    db = live_server["db"]
    photo_ids = live_server["data"]["photos"][:4]
    _write_grouped_pipeline_cache(live_server, photo_ids)
    photo_id = photo_ids[0]
    before_rating = db.conn.execute(
        "SELECT rating FROM photos WHERE id = ?", (photo_id,)
    ).fetchone()["rating"]
    before_wildlife = db.conn.execute(
        "SELECT wildlife_excluded FROM photos WHERE id = ?", (photo_id,)
    ).fetchone()["wildlife_excluded"]
    attempted_rating = next(v for v in range(6) if v != before_rating)

    page.goto(f"{live_server['url']}/pipeline/review")
    expect(page.locator(".photo-card[data-photo-id]")).to_have_count(4)
    page.evaluate("openGroupReview(0, 0)")
    page.wait_for_function("grmState && grmState.seeded === true")
    page.evaluate(
        """() => {
          window.__originalSafeFetchForApplyRating = window.safeFetch;
          window.__applyRatingWriteCalls = [];
          window.safeFetch = function(url) {
            if (url === '/api/pipeline/group/apply') {
              const args = arguments;
              return new Promise((resolve, reject) => {
                window.__releaseApplyRatingLock = () => {
                  window.__originalSafeFetchForApplyRating
                    .apply(window, args).then(resolve, reject);
                };
              });
            }
            if (url === '/api/batch/rating' ||
                (typeof url === 'string' &&
                 url.indexOf('/wildlife_excluded') !== -1)) {
              window.__applyRatingWriteCalls.push(url);
            }
            return window.__originalSafeFetchForApplyRating
              .apply(this, arguments);
          };
          grmMoveReject();
          window.__applyRatingSettled = false;
          window.__applyRatingPromise = grmApply().then(
            () => { window.__applyRatingSettled = true; },
            error => {
              window.__applyRatingError = String(error);
              window.__applyRatingSettled = true;
            }
          );
        }"""
    )
    page.wait_for_function(
        "typeof window.__releaseApplyRatingLock === 'function'"
    )
    assert page.evaluate("grmState.applying") is True

    blocked = page.evaluate(
        """async args => ({
          rating: await setRatingFor(args.photoId, args.rating),
          wildlife: await setWildlifeExcludedFor(args.photoId, true),
        })""",
        {"photoId": photo_id, "rating": attempted_rating},
    )
    assert blocked == {"rating": False, "wildlife": False}
    assert page.evaluate("window.__applyRatingWriteCalls") == []
    assert db.conn.execute(
        "SELECT rating FROM photos WHERE id = ?", (photo_id,)
    ).fetchone()["rating"] == before_rating
    assert db.conn.execute(
        "SELECT wildlife_excluded FROM photos WHERE id = ?", (photo_id,)
    ).fetchone()["wildlife_excluded"] == before_wildlife
    cached = page.evaluate(
        "pid => pipelineResults.photos.find(photo => photo.id === pid)",
        photo_id,
    )
    assert cached["rating"] == before_rating
    assert (cached.get("wildlife_excluded") or 0) == (before_wildlife or 0)

    try:
        page.evaluate("window.__releaseApplyRatingLock()")
        page.wait_for_function("window.__applyRatingSettled === true")
        assert page.evaluate("window.__applyRatingError || null") is None
        after = page.evaluate(
            """async args => ({
              rating: await setRatingFor(args.photoId, args.rating),
            })""",
            {"photoId": photo_id, "rating": attempted_rating},
        )
        assert after == {"rating": True}
        assert db.conn.execute(
            "SELECT rating FROM photos WHERE id = ?", (photo_id,)
        ).fetchone()["rating"] == attempted_rating
    finally:
        page.evaluate(
            "() => { window.safeFetch = window.__originalSafeFetchForApplyRating; }"
        )


def test_history_survives_delay_reload_and_repeated_undo_redo(live_server, page):
    db = live_server['db']
    ids = live_server['data']['photos'][:4]
    _write_grouped_pipeline_cache(live_server, ids)
    page.goto(f"{live_server['url']}/pipeline/review")
    page.get_by_test_id('reject-burst').first.click()
    expect(page.locator('#undoMsg')).to_have_text('Set flag to rejected on 2 photos')
    page.get_by_test_id('reject-burst').nth(1).click()
    expect(page.get_by_test_id('reject-burst').nth(1)).to_have_attribute('aria-label', 'Clear rejects')
    expect(page.locator('#historyUndoBtn')).to_be_enabled()
    assert _flags(db, ids) == ['rejected'] * 4

    # The old page-local history expired after five seconds.
    page.clock.install()
    page.clock.fast_forward(6000)
    expect(page.locator('#historyUndoBtn')).to_be_enabled()
    page.reload()
    expect(page.locator('#historyUndoBtn')).to_be_enabled()
    page.locator('#historyUndoBtn').click()
    expect(page.get_by_test_id('reject-burst').nth(1)).to_have_attribute('aria-label', 'Reject burst')
    assert _flags(db, ids) == ['rejected', 'rejected', 'none', 'none']
    expect(page.locator('#historyUndoBtn')).to_be_enabled()
    page.keyboard.press('Control+z')
    expect(page.get_by_test_id('reject-burst').first).to_have_attribute('aria-label', 'Reject burst')
    assert _flags(db, ids) == ['none'] * 4
    expect(page.locator('#historyUndoBtn')).to_be_disabled()
    expect(page.locator('#historyRedoBtn')).to_be_enabled()
    page.keyboard.press('Meta+Shift+z')
    expect(page.get_by_test_id('reject-burst').first).to_have_attribute('aria-label', 'Clear rejects')
    expect(page.locator('#historyRedoBtn')).to_be_enabled()
    page.locator('#historyRedoBtn').click()
    expect(page.get_by_test_id('reject-burst').nth(1)).to_have_attribute('aria-label', 'Clear rejects')
    assert _flags(db, ids) == ['rejected'] * 4
    expect(page.locator('#historyRedoBtn')).to_be_disabled()


def test_grouping_and_photo_edit_undo_in_order_in_browser(live_server, page):
    db = live_server['db']
    ids = live_server['data']['photos'][:4]
    _write_grouped_pipeline_cache(live_server, ids)
    page.goto(f"{live_server['url']}/pipeline/review")
    expect(page.get_by_test_id('reject-burst')).to_have_count(2)
    page.evaluate('detachBurst(0, 0)')
    expect(page.get_by_test_id('reject-encounter')).to_have_count(2)
    page.evaluate('(id) => setPipelineReviewFlag(id, "flagged")', ids[0])
    expect(page.locator('#undoMsg')).to_have_text('Set flag to flagged')
    page.locator('#historyUndoBtn').click()
    expect(page.locator('#undoMsg')).to_have_text('Burst detached from encounter')
    assert db.get_photo(ids[0])['flag'] == 'none'
    expect(page.get_by_test_id('reject-encounter')).to_have_count(2)
    page.locator('#historyUndoBtn').click()
    expect(page.get_by_test_id('reject-encounter')).to_have_count(1)
    expect(page.locator('#historyUndoBtn')).to_be_disabled()


def test_failed_undo_remains_retryable_and_double_click_is_serialized(live_server, page):
    db = live_server['db']
    ids = live_server['data']['photos'][:4]
    _write_grouped_pipeline_cache(live_server, ids)
    page.goto(f"{live_server['url']}/pipeline/review")
    page.get_by_test_id('reject-burst').first.click()
    expect(page.locator('#historyUndoBtn')).to_be_enabled()
    page.route('**/api/undo', lambda route: route.fulfill(
        status=503, content_type='application/json', body='{"error":"Try again"}'
    ))
    page.locator('#historyUndoBtn').click()
    expect(page.get_by_text('Try again', exact=True)).to_be_visible()
    expect(page.locator('#historyUndoBtn')).to_be_enabled()
    assert _flags(db, ids) == ['rejected', 'rejected', 'none', 'none']
    page.unroute('**/api/undo')
    requests = []
    page.on('request', lambda request: requests.append(request.url) if request.url.endswith('/api/undo') else None)
    page.evaluate('Promise.all([doUndo(), doUndo()])')
    assert len(requests) == 1
    expect(page.get_by_test_id('reject-burst').first).to_have_attribute('aria-label', 'Reject burst')
    expect(page.locator('#historyUndoBtn')).to_be_disabled()
    assert _flags(db, ids) == ['none'] * 4


def test_text_undo_does_not_undo_photos(live_server, page):
    db = live_server['db']
    ids = live_server['data']['photos'][:4]
    _write_grouped_pipeline_cache(live_server, ids)
    page.goto(f"{live_server['url']}/pipeline/review")
    page.get_by_test_id('reject-burst').first.click()
    expect(page.locator('#historyUndoBtn')).to_be_enabled()
    field = page.locator('#speciesFilterInput')
    field.focus()
    field.press_sequentially('bird')
    field.press('Control+z')
    assert _flags(db, ids) == ['rejected', 'rejected', 'none', 'none']
    expect(page.locator('#historyUndoBtn')).to_be_enabled()


def test_culling_can_be_undone_without_leaving_the_page(live_server, page):
    db = live_server['db']
    ids = live_server['data']['photos'][:4]
    db.update_photo_flag(ids[0], 'flagged')
    _write_grouped_pipeline_cache(live_server, ids)
    path = os.path.join(os.path.dirname(db._db_path), f'pipeline_results_ws{db._ws_id()}.json')
    with open(path) as cache_file:
        cache = json.load(cache_file)
    for photo in cache['photos']:
        photo['label'] = 'KEEP' if photo['id'] == ids[1] else 'REJECT'
    with open(path, 'w') as cache_file:
        json.dump(cache, cache_file)
    page.goto(f"{live_server['url']}/cull")
    page.on('dialog', lambda dialog: dialog.accept())
    # ids[0] was flagged before this run. That decision is already applied, so
    # the page shows it as a keeper instead of the fresh REJECT suggestion,
    # and applying leaves the flag alone rather than overwriting it.
    expect(page.locator(f'.cull-card[data-photo-id="{ids[0]}"]')).to_have_class(re.compile(r'\bkeep\b'))
    page.locator('#applyBtn').click()
    expect(page.locator('#cullStatus')).to_contain_text('Applied!')
    expect(page.locator('#historyUndoBtn')).to_be_enabled()
    assert _flags(db, ids) == ['flagged', 'flagged', 'rejected', 'rejected']
    page.locator('#historyUndoBtn').click()
    expect(page.locator('#cullStatus')).to_contain_text('Undone: Culling')
    assert page.url.endswith('/cull')
    assert _flags(db, ids) == ['flagged', 'none', 'none', 'none']
    # Undo took the flags back off, so every card except the still-flagged
    # ids[0] falls back to what this run's analysis suggests.
    expect(page.locator(f'.cull-card[data-photo-id="{ids[0]}"]')).to_have_class(re.compile(r'\bkeep\b'))
    expect(page.locator(f'.cull-card[data-photo-id="{ids[1]}"]')).to_have_class(re.compile(r'\bkeep\b'))
    for pid in ids[2:]:
        expect(page.locator(f'.cull-card[data-photo-id="{pid}"]')).to_have_class(re.compile(r'\breject\b'))
    expect(page.locator('#historyRedoBtn')).to_be_enabled()
    page.locator('#historyRedoBtn').click()
    expect(page.locator('#cullStatus')).to_contain_text('Redone: Culling')
    assert _flags(db, ids) == ['flagged', 'flagged', 'rejected', 'rejected']

    page.locator('#historyUndoBtn').click()
    expect(page.locator('#cullStatus')).to_contain_text('Undone: Culling')
    page.locator('#applyBtn').click()
    expect(page.locator('#cullStatus')).to_contain_text('Applied!')
    assert _flags(db, ids) == ['flagged', 'flagged', 'rejected', 'rejected']


def test_history_panel_undo_labels_the_actual_reversible_edit(live_server, page):
    db = live_server['db']
    ids = live_server['data']['photos'][:4]
    _write_grouped_pipeline_cache(live_server, ids)
    page.goto(f"{live_server['url']}/pipeline/review")
    page.get_by_test_id('reject-burst').first.click()
    expect(page.locator('#historyUndoBtn')).to_be_enabled()
    db.record_edit('prediction_reject', 'Declined a suggestion', '', [])
    page.evaluate("toggleBottomPanel(); switchBpTab('history')")
    row = page.locator('.bp-history-row').filter(has_text='Set flag to rejected')
    expect(row.get_by_role('button', name='Undo', exact=True)).to_be_visible()
    declined = page.locator('.bp-history-row').filter(has_text='Declined a suggestion')
    expect(declined.get_by_role('button', name='Undo', exact=True)).to_have_count(0)
    row.get_by_role('button', name='Undo', exact=True).click()
    expect(page.locator('#historyUndoBtn')).to_be_disabled()
    assert _flags(db, ids) == ['none'] * 4


def test_undo_refreshes_open_lightbox_flag(live_server, page):
    db = live_server['db']
    ids = live_server['data']['photos'][:4]
    _write_grouped_pipeline_cache(live_server, ids)
    page.goto(f"{live_server['url']}/pipeline/review")
    expect(page.get_by_test_id('reject-burst')).to_have_count(2)
    page.evaluate('(id) => setPipelineReviewFlag(id, "flagged")', ids[0])
    expect(page.locator('#historyUndoBtn')).to_be_enabled()
    page.evaluate('(id) => openLightbox(id, "hawk1.jpg", [{id: id, flag: "flagged"}])', ids[0])
    expect(page.locator('#lightboxFlagBtn')).to_have_attribute('aria-pressed', 'true')
    page.keyboard.press('Meta+z')
    expect(page.locator('#lightboxFlagBtn')).to_have_attribute('aria-pressed', 'false')
    assert db.get_photo(ids[0])['flag'] == 'none'


def test_group_review_removal_is_undoable_without_a_flag_change(live_server, page):
    ids = live_server['data']['photos'][:4]
    _write_grouped_pipeline_cache(live_server, ids)
    page.goto(live_server['url'] + '/pipeline/review')
    expect(page.locator('.photo-card[data-photo-id]')).to_have_count(4)
    page.evaluate('openGroupReview(0, 0)')
    page.wait_for_function('grmState.seeded === true')
    page.evaluate('(pid) => grmRemoveFromGroup([pid])', ids[1])
    page.locator('#grmConfirmSpeciesChk').uncheck()
    page.locator('#grmApplyFlagsChk').check()
    page.evaluate('grmApply()')
    expect(page.locator('#grmOverlay')).not_to_have_class(re.compile(r'\bopen\b'))
    expect(page.get_by_test_id('reject-burst')).to_have_count(3)
    expect(page.locator('#historyUndoBtn')).to_be_enabled()
    page.locator('#historyUndoBtn').click()
    expect(page.get_by_test_id('reject-burst')).to_have_count(2)
    page.locator('#historyRedoBtn').click()
    expect(page.get_by_test_id('reject-burst')).to_have_count(3)


def test_rapid_review_refreshes_saved_decisions_after_history(live_server, page):
    ids = live_server['data']['photos'][:4]
    _write_grouped_pipeline_cache(live_server, ids)
    page.goto(live_server['url'] + '/pipeline/rapid-review')
    page.wait_for_function('rapid.seeded')
    page.evaluate("changeQueue({filter: 'all'})")
    page.wait_for_function('rapid.seeded')
    page.evaluate("decideCurrent('reject')")
    page.evaluate('applyCurrent(false)')
    page.wait_for_function('rapid.seeded && !rapid.applying')
    rejected = page.evaluate("Object.values(rapid.photoMap).find(p => p.flag === 'rejected').id")
    expect(page.locator('#historyUndoBtn')).to_be_enabled()
    page.locator('#historyUndoBtn').click()
    page.wait_for_function('(id) => rapid.photoMap[id].flag === "none" && rapid.seeded', arg=rejected)
    assert page.evaluate('(id) => rapid.rejects.has(id)', rejected) is False
    page.locator('#historyRedoBtn').click()
    page.wait_for_function('(id) => rapid.photoMap[id].flag === "rejected" && rapid.seeded', arg=rejected)
    assert page.evaluate('(id) => rapid.rejects.has(id)', rejected) is True
    # Staged local choices must not undo saved database edits behind the page.
    page.evaluate("decideCurrent('pick')")
    page.evaluate('doUndo()')
    assert live_server['db'].get_photo(rejected)['flag'] == 'rejected'


@pytest.mark.parametrize('scoped', [False, True])
@pytest.mark.parametrize('history_action', ['rating', 'flag'])
def test_cull_history_preserves_analysis_scope_and_unrelated_suggestions(live_server, page, scoped, history_action):
    ids = live_server['data']['photos'][:4]
    _write_grouped_pipeline_cache(live_server, ids)
    page.goto(live_server['url'] + '/cull')
    expect(page.locator('.cull-card')).to_have_count(4)
    page.evaluate('''({ids, scoped}) => {
      const results = JSON.parse(JSON.stringify(pipelineResults));
      results.photos[0].label = 'KEEP';
      results.photos[1].label = 'REJECT';
      if (scoped) {
        selectedCollectionId = 123;
        results.photos = results.photos.filter(photo => ids.slice(0, 2).includes(photo.id));
        results.encounters = ids.slice(0, 2).map(id => ({
          photo_ids: [id], bursts: [{photo_ids: [id]}],
          photo_count: 1, burst_count: 1, species: ['Scoped bird']
        }));
      }
      setPipelineResults(results);
      window.cullAnalysisBefore = JSON.stringify(pipelineResults.encounters);
    }''', {'ids': ids, 'scoped': scoped})
    if not scoped:
        # The unscoped read returns the same analysis, with live fields attached.
        snapshot = page.evaluate('pipelineResults')
        page.route('**/api/pipeline/page-init', lambda route: route.fulfill(json={'results': snapshot}))
    page.evaluate('''({id, action}) => safeFetch('/api/photos/' + id + '/' + action, {
      method: 'POST', headers: {'Content-Type': 'application/json'},
      body: JSON.stringify({[action]: action === 'rating' ? 1 : 'rejected'})
    })''', {'id': ids[0], 'action': history_action})
    expect(page.locator('#historyUndoBtn')).to_be_enabled()
    page.locator('#historyUndoBtn').click()
    expect(page.locator('#historyRedoBtn')).to_be_enabled()
    # Undoing an unrelated edit — a rating or one photo's flag — leaves both
    # cards on this run's suggestion. Neither photo carries a saved flag once
    # the undo lands, so nothing overrides the analysis.
    actions = ['keep', 'reject']
    for pid, action in zip(ids[:2], actions, strict=True):
        expect(page.locator(f'.cull-card[data-photo-id="{pid}"]')).to_have_class(re.compile(r'\b' + action + r'\b'))
    assert page.evaluate('JSON.stringify(pipelineResults.encounters) === window.cullAnalysisBefore')


def test_rapid_review_can_retry_a_failed_history_refresh(live_server, page):
    ids = live_server['data']['photos'][:4]
    _write_grouped_pipeline_cache(live_server, ids)
    page.goto(live_server['url'] + '/pipeline/rapid-review')
    page.wait_for_function('rapid.seeded')
    page.evaluate("changeQueue({filter: 'all'})")
    page.wait_for_function('rapid.seeded')
    page.evaluate("decideCurrent('reject')")
    page.evaluate('applyCurrent(false)')
    page.wait_for_function('rapid.seeded && !rapid.applying')
    rejected = page.evaluate("Object.values(rapid.photoMap).find(p => p.flag === 'rejected').id")
    page.route('**/api/pipeline/results', lambda route: route.fulfill(status=503, json={'error': 'Try again'}))
    page.locator('#historyUndoBtn').click()
    retry = page.get_by_role('button', name='Retry', exact=True)
    expect(retry).to_be_visible()
    page.evaluate('doRedo()')
    expect(page.get_by_text('Rapid Review could not load. Use Retry before undoing or redoing saved edits.', exact=True)).to_be_visible()
    assert live_server['db'].get_photo(rejected)['flag'] == 'none'
    page.unroute('**/api/pipeline/results')
    retry.click()
    page.wait_for_function('rapid.seeded && !rapid.applying && !rapid.loadError')
    assert page.evaluate('(id) => rapid.photoMap[id].flag', rejected) == 'none'
    page.locator('#historyRedoBtn').click()
    page.wait_for_function('(id) => rapid.photoMap[id].flag === "rejected" && rapid.seeded', arg=rejected)


@pytest.mark.parametrize('scoped', [False, True])
def test_cull_apply_keeps_saved_decisions_after_rating_undo(live_server, page, scoped):
    ids = live_server['data']['photos'][:4]
    _write_grouped_pipeline_cache(live_server, ids)
    page.goto(live_server['url'] + '/cull')
    expect(page.locator('.cull-card')).to_have_count(4)
    page.evaluate('''({scoped}) => {
      pipelineResults.photos.forEach(photo => { photo.label = 'REJECT'; });
      if (scoped) selectedCollectionId = 123;
      rebuildCullDataFromPipeline();
    }''', {'scoped': scoped})
    # Decide by clicking, the way the user does, so the decisions are pinned:
    # every card starts on REJECT, one click moves it to Keep, two to Review.
    page.click(f'.cull-card[data-photo-id="{ids[0]}"] .cull-card-action')
    expect(page.locator(f'.cull-card[data-photo-id="{ids[0]}"]')).to_have_class(re.compile(r'\bkeep\b'))
    for pid in ids[1:]:
        page.click(f'.cull-card[data-photo-id="{pid}"] .cull-card-action')
        page.click(f'.cull-card[data-photo-id="{pid}"] .cull-card-action')
        expect(page.locator(f'.cull-card[data-photo-id="{pid}"]')).to_have_class(re.compile(r'\breview\b'))
    page.on('dialog', lambda dialog: dialog.accept())
    page.locator('#applyBtn').click()
    expect(page.locator('#cullStatus')).to_contain_text('Applied!')
    assert page.evaluate('cullDirty') is False
    assert page.evaluate('id => pipelineResults.photos.find(p => p.id === id).flag', ids[0]) == 'flagged'
    page.evaluate('''id => safeFetch('/api/photos/' + id + '/rating', {
      method: 'POST', headers: {'Content-Type': 'application/json'}, body: JSON.stringify({rating: 1})
    })''', ids[0])
    expect(page.locator('#historyUndoBtn')).to_be_enabled()
    page.locator('#historyUndoBtn').click()
    expect(page.locator('#historyRedoBtn')).to_be_enabled()
    expect(page.locator(f'.cull-card[data-photo-id="{ids[0]}"]')).to_have_class(re.compile(r'\bkeep\b'))
    for pid in ids[1:]:
        expect(page.locator(f'.cull-card[data-photo-id="{pid}"]')).to_have_class(re.compile(r'\breview\b'))


@pytest.mark.parametrize('analysis', ['doReflow', 'doRegroupLive', 'runCulling', 'onScoringChange', 'onGroupingChange'])
@pytest.mark.parametrize('succeeds', [True, False])
def test_cull_pending_analysis_blocks_history_until_response(live_server, page, analysis, succeeds):
    ids = live_server['data']['photos'][:4]
    _write_grouped_pipeline_cache(live_server, ids)
    page.goto(live_server['url'] + '/cull')
    expect(page.locator('.cull-card')).to_have_count(4)
    page.evaluate('''id => safeFetch('/api/photos/' + id + '/flag', {
      method: 'POST', headers: {'Content-Type': 'application/json'}, body: JSON.stringify({flag: 'flagged'})
    })''', ids[0])
    expect(page.locator('#historyUndoBtn')).to_be_enabled()
    snapshot = page.evaluate('pipelineResults')
    held = []
    endpoint = 'reflow' if analysis in ('doReflow', 'onScoringChange') else 'regroup-live'
    page.route('**/api/pipeline/' + endpoint, lambda route: held.append(route))
    page.evaluate('''analysis => {
      selectedCollectionId = 123;
      const slider = document.getElementById(analysis === 'onScoringChange' ? 'slRejectFocus' : 'slWTime');
      window.pendingAnalysis = window[analysis](slider);
    }''', analysis)
    expect(page.locator('#applyBtn')).to_be_disabled()
    assert page.evaluate('doUndo()') is False
    assert live_server['db'].get_photo(ids[0])['flag'] == 'flagged'
    for _ in range(100):
        if held:
            break
        page.wait_for_timeout(50)
    assert len(held) == 1
    if succeeds:
        held[0].fulfill(json=snapshot)
    else:
        held[0].abort()
    page.evaluate('window.pendingAnalysis')
    page.wait_for_function('cullAnalysisPending === 0 && _reflowTimer == null && _regroupTimer == null')
    expect(page.locator('#applyBtn')).to_be_enabled()
    assert page.evaluate('doUndo()') is True
    assert live_server['db'].get_photo(ids[0])['flag'] == 'none'
    expect(page.locator(f'.cull-card[data-photo-id="{ids[0]}"]')).to_have_class(re.compile(r'\breview\b'))


def test_stale_undo_refreshes_the_advertised_action_without_undoing_another(live_server, page):
    ids = live_server['data']['photos'][:4]
    db = live_server['db']
    _write_grouped_pipeline_cache(live_server, ids)
    page.goto(live_server['url'] + '/pipeline/review')
    page.evaluate('''async id => {
      await safeFetch('/api/photos/' + id + '/flag', {
        method: 'POST', headers: {'Content-Type': 'application/json'}, body: JSON.stringify({flag: 'flagged'})
      });
      await safeFetch('/api/pipeline/detach-burst', {
        method: 'POST', headers: {'Content-Type': 'application/json'},
        body: JSON.stringify({encounter_index: 0, burst_index: 0})
      });
    }''', ids[0])
    button = page.locator('#historyUndoBtn')
    expect(button).to_have_attribute('title', re.compile('detached', re.I))
    path = os.path.join(os.path.dirname(db._db_path), f'pipeline_results_ws{db._ws_id()}.json')
    with open(path) as cache_file:
        newer = json.load(cache_file)
    newer['encounters'][0]['bursts'].append({'photo_ids': []})
    with open(path, 'w') as cache_file:
        json.dump(newer, cache_file)
    button.click()
    expect(button).to_be_enabled()
    expect(button).to_have_attribute('title', re.compile('flag', re.I))
    assert db.get_photo(ids[0])['flag'] == 'flagged'
    button.click()
    expect(page.locator('#historyRedoBtn')).to_be_enabled()
    assert db.get_photo(ids[0])['flag'] == 'none'
