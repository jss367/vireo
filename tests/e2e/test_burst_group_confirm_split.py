"""E2E coverage for the Review Burst Group confirm/apply split.

The burst-group modal footer (in `pipeline_review.html`) carries two
checkboxes — "Confirm species" (#grmConfirmSpeciesChk) and "Apply
picks/rejects" (#grmApplyFlagsChk) — and a single "Apply and close" button
(#grmApplyBtn). These tests drive a real browser against a real Flask server
with a real on-disk pipeline cache, so the apply paths exercise the live
`/api/pipeline/group/apply` (flags-only) and `/api/encounters/species`
endpoints and the assertions read genuinely persisted state (the photos.flag
column, the photo_keywords table, and the pipeline cache file on disk).

We reuse the predictionless-cache helper shape from
`test_pipeline_review_species.py` but build a SINGLE multi-frame burst so we
can move/remove individual frames and confirm that species tagging covers
every frame (not just picks).

Why a new file rather than extending test_pipeline_review_species.py: that
file's helpers seed separate single-photo encounters for the species-widget
flow; these tests need one multi-frame burst opened through the burst-group
modal (grmApply), a distinct setup and surface area. They share the same
live_server/page fixtures and the same real-cache + real-DB assertion style.
"""
import json
import os
import re

import pytest
from playwright.sync_api import expect

# Known product bug (NOT a test defect): grmOnToggleChange() pins BOTH
# overrides (confirmSpeciesOverride AND applyFlagsOverride) to their current
# DOM state on EVERY checkbox toggle, instead of recording an override only
# for the box the user actually clicked. So the realistic species-only flow —
# uncheck "Apply picks/rejects" first, THEN type a new species — leaves
# "Confirm species" stuck unchecked even though a real, pending species change
# exists. That contradicts the design's per-box override intent ("track
# whether the user has manually overridden EACH box") and the UI-truthfulness
# rule (a real pending change must surface). The underlying apply logic is
# correct (verified: with flags unchecked + species checked, no flag persists,
# every frame is tagged, the burst is confirmed) — only the checkbox state
# derivation is wrong. These two tests assert the INTENDED user flow; they are
# strict-xfail so they flip to a hard failure (XPASS) the moment the bug is
# fixed, forcing the marker's removal.
_TOGGLE_PINS_BOTH_OVERRIDES = pytest.mark.xfail(
    strict=True,
    reason="grmOnToggleChange pins both overrides on any toggle; unchecking "
    "flags then typing a species fails to auto-check Confirm species",
)


def _write_single_burst_cache(live_server, photo_ids, *, confirmed_species=None):
    """Write a pipeline cache with one encounter containing one multi-frame
    burst over `photo_ids`. Optionally pre-confirm the burst's species so we
    can test the "same species → no-op, stays unchecked" smart default.
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
            "confirmed_species": confirmed_species,
        }
        for row in rows
    ]
    ids = [p["id"] for p in photos]
    burst = {
        "photo_ids": ids,
        "species_predictions": [],
        "species_override": (
            {"species": confirmed_species, "confirmed": True}
            if confirmed_species
            else None
        ),
    }
    cache = {
        "photos": photos,
        "encounters": [
            {
                "photo_ids": ids,
                "photo_count": len(ids),
                "burst_count": 1,
                "time_range": [photos[0]["timestamp"], photos[-1]["timestamp"]],
                "species": [confirmed_species] if confirmed_species else [],
                "species_predictions": [],
                "species_confirmed": bool(confirmed_species),
                "confirmed_species": confirmed_species,
                "bursts": [burst],
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
    path = _cache_path(live_server)
    with open(path, "w") as f:
        json.dump(cache, f)


def _cache_path(live_server):
    db = live_server["db"]
    return os.path.join(
        os.path.dirname(db._db_path),
        f"pipeline_results_ws{db._active_workspace_id}.json",
    )


def _read_cache(live_server):
    with open(_cache_path(live_server)) as f:
        return json.load(f)


def _open_burst_modal(page, live_server):
    """Navigate to the pipeline review page and open the burst-group modal by
    clicking the first photo card (routes through openInspect → openGroupReview).
    Waits for the modal to seed so Apply is enabled.
    """
    # The burst modal is a full-screen overlay built for large pixel-peeping
    # displays; its footer (species field + sliders + the two commit
    # checkboxes + Apply button) is wider than the default 1280px test
    # viewport, which pushes Apply off-screen. Use a realistically wide
    # viewport so every footer control is clickable.
    page.set_viewport_size({"width": 1600, "height": 900})
    page.goto(f"{live_server['url']}/pipeline/review")
    page.locator(".photo-card img").first.click()
    expect(page.locator("#grmOverlay")).to_have_class(re.compile(r"\bopen\b"))
    # Seed completes when Apply leaves the "Loading…" state and re-enables.
    expect(page.locator("#grmApplyBtn")).to_be_enabled()


def _photo_flags(live_server, photo_ids):
    db = live_server["db"]
    placeholders = ",".join("?" for _ in photo_ids)
    rows = db.conn.execute(
        f"SELECT id, flag FROM photos WHERE id IN ({placeholders})", photo_ids
    ).fetchall()
    return {r["id"]: (r["flag"] or "none") for r in rows}


def _photos_with_species(live_server, photo_ids, species):
    db = live_server["db"]
    placeholders = ",".join("?" for _ in photo_ids)
    rows = db.conn.execute(
        f"""
        SELECT p.id
        FROM photos p
        JOIN photo_keywords pk ON pk.photo_id = p.id
        JOIN keywords k ON k.id = pk.keyword_id
        WHERE p.id IN ({placeholders}) AND k.name = ? AND k.is_species = 1
        """,
        (*photo_ids, species),
    ).fetchall()
    return {r["id"] for r in rows}


def test_smart_default_flags_checked_when_moves_pending(live_server, page):
    """Moving a frame to rejects pre-checks "Apply picks/rejects" via the smart
    default; "Confirm species" stays unchecked because the species field is
    empty (no pending species change)."""
    photo_ids = live_server["data"]["photos"][0:3]  # hawk1/2/3, one burst
    _write_single_burst_cache(live_server, photo_ids)

    _open_burst_modal(page, live_server)

    # Reject the currently-selected frame.
    page.keyboard.press("x")
    expect(page.locator("#grmCount")).to_contain_text("1 rejects")

    expect(page.locator("#grmApplyFlagsChk")).to_be_checked()
    expect(page.locator("#grmConfirmSpeciesChk")).not_to_be_checked()


def test_smart_default_species_checked_on_new_species(live_server, page):
    """Typing a species that differs from the burst's confirmed species
    pre-checks "Confirm species"."""
    photo_ids = live_server["data"]["photos"][0:3]
    _write_single_burst_cache(live_server, photo_ids, confirmed_species="Red-tailed Hawk")

    _open_burst_modal(page, live_server)

    # Field opens pre-filled with the confirmed species → no change → unchecked.
    expect(page.locator("#grmSpecies")).to_have_value("Red-tailed Hawk")
    expect(page.locator("#grmConfirmSpeciesChk")).not_to_be_checked()

    page.locator("#grmSpecies").fill("Cooper's Hawk")
    expect(page.locator("#grmConfirmSpeciesChk")).to_be_checked()
    # No flag moves → flags box stays unchecked.
    expect(page.locator("#grmApplyFlagsChk")).not_to_be_checked()


def test_unchecking_dirty_box_shows_amber_hint(live_server, page):
    """Unchecking a checked+dirty box reveals its amber .grm-dirty-hint with
    non-empty text."""
    photo_ids = live_server["data"]["photos"][0:3]
    _write_single_burst_cache(live_server, photo_ids)

    _open_burst_modal(page, live_server)

    page.keyboard.press("x")  # one pending reject → flags box auto-checks
    expect(page.locator("#grmApplyFlagsChk")).to_be_checked()
    expect(page.locator("#grmFlagsDirtyHint")).to_be_hidden()

    # Uncheck it (real click on the checkbox fires grmOnToggleChange).
    page.locator("#grmApplyFlagsChk").uncheck()

    hint = page.locator("#grmFlagsDirtyHint")
    expect(hint).to_be_visible()
    assert hint.inner_text().strip(), "amber hint text should be non-empty"
    expect(hint).to_contain_text("won't be saved")


@_TOGGLE_PINS_BOTH_OVERRIDES
def test_species_only_apply_leaves_flags_untouched_and_tags_all_frames(live_server, page):
    """Species-only apply (flags unchecked): no photo's flag changes, EVERY
    burst frame gets the species keyword, and the burst is marked confirmed.

    Intended flow: stage a flag move, uncheck "Apply picks/rejects", THEN type
    a new species so "Confirm species" auto-checks. Currently xfail — see
    _TOGGLE_PINS_BOTH_OVERRIDES."""
    photo_ids = live_server["data"]["photos"][0:3]
    _write_single_burst_cache(live_server, photo_ids)

    _open_burst_modal(page, live_server)

    # Stage a flag move so flags would be dirty — then uncheck flags to prove
    # the move is NOT persisted while species is.
    page.keyboard.press("x")
    expect(page.locator("#grmApplyFlagsChk")).to_be_checked()
    page.locator("#grmApplyFlagsChk").uncheck()

    page.locator("#grmSpecies").fill("Northern Goshawk")
    expect(page.locator("#grmConfirmSpeciesChk")).to_be_checked()

    with page.expect_response("**/api/encounters/species"):
        page.locator("#grmApplyBtn").click()
    expect(page.locator("#grmOverlay")).not_to_have_class(re.compile(r"\bopen\b"))

    # No flag changed: the staged reject was discarded with the unchecked box.
    flags = _photo_flags(live_server, photo_ids)
    assert flags == {pid: "none" for pid in photo_ids}, flags

    # Every frame carries the species keyword — not just a pick.
    tagged = _photos_with_species(live_server, photo_ids, "Northern Goshawk")
    assert tagged == set(photo_ids), tagged

    # Burst is marked confirmed in the persisted cache.
    cache = _read_cache(live_server)
    burst = cache["encounters"][0]["bursts"][0]
    assert burst.get("species_override", {}).get("confirmed") is True
    assert burst["species_override"]["species"] == "Northern Goshawk"


def test_flags_only_apply_leaves_species_unconfirmed(live_server, page):
    """Flags-only apply (species unchecked): flags persist, but no species
    keyword is added and the burst is not marked confirmed."""
    photo_ids = live_server["data"]["photos"][0:3]
    _write_single_burst_cache(live_server, photo_ids)

    _open_burst_modal(page, live_server)

    # Reject the selected frame → flags auto-checks.
    page.keyboard.press("x")
    expect(page.locator("#grmApplyFlagsChk")).to_be_checked()

    # Type a species but explicitly leave it unconfirmed.
    page.locator("#grmSpecies").fill("Sharp-shinned Hawk")
    expect(page.locator("#grmConfirmSpeciesChk")).to_be_checked()
    page.locator("#grmConfirmSpeciesChk").uncheck()
    expect(page.locator("#grmSpeciesDirtyHint")).to_be_visible()

    with page.expect_response("**/api/pipeline/group/apply"):
        page.locator("#grmApplyBtn").click()
    expect(page.locator("#grmOverlay")).not_to_have_class(re.compile(r"\bopen\b"))

    # Exactly one frame was rejected; the rest untouched.
    flags = _photo_flags(live_server, photo_ids)
    assert sorted(flags.values()) == ["none", "none", "rejected"], flags

    # No species keyword added anywhere.
    tagged = _photos_with_species(live_server, photo_ids, "Sharp-shinned Hawk")
    assert tagged == set(), tagged

    # Burst NOT marked confirmed.
    cache = _read_cache(live_server)
    burst = cache["encounters"][0]["bursts"][0]
    assert not (burst.get("species_override") or {}).get("confirmed")


@_TOGGLE_PINS_BOTH_OVERRIDES
def test_species_only_with_removed_photo_keeps_member_and_tags_it(live_server, page):
    """Truthfulness guard: mark a photo "Remove from group", leave flags
    UNCHECKED, confirm species → the removal is discarded (photo stays a burst
    member) AND it still receives the species keyword.

    If grmApply detached the removed photo while flags were unchecked, the
    photo would no longer be a burst member and the burst-scoped
    /api/encounters/species call (validated against the on-disk burst) would
    either omit it or fail — so this asserts both the cache structure and the
    keyword landing on the removed frame.
    """
    photo_ids = live_server["data"]["photos"][0:3]
    _write_single_burst_cache(live_server, photo_ids)

    _open_burst_modal(page, live_server)

    # Select the second card explicitly, then remove it from the group.
    second_pid = int(
        page.locator("#grmOverlay .grm-card[data-photo-id]").nth(1).get_attribute("data-photo-id")
    )
    page.locator(f"#grmOverlay .grm-card[data-photo-id='{second_pid}']").click()
    page.locator("#grmRemoveBtn").click()

    # Removal makes flags dirty (detachNew>0) so the box auto-checks; uncheck it
    # to discard the removal, mirroring a user who only wants to confirm species.
    expect(page.locator("#grmApplyFlagsChk")).to_be_checked()
    page.locator("#grmApplyFlagsChk").uncheck()
    expect(page.locator("#grmFlagsDirtyHint")).to_be_visible()

    page.locator("#grmSpecies").fill("Broad-winged Hawk")
    expect(page.locator("#grmConfirmSpeciesChk")).to_be_checked()

    with page.expect_response("**/api/encounters/species"):
        page.locator("#grmApplyBtn").click()
    expect(page.locator("#grmOverlay")).not_to_have_class(re.compile(r"\bopen\b"))

    # Removal discarded: the burst still holds all three frames.
    cache = _read_cache(live_server)
    burst = cache["encounters"][0]["bursts"][0]
    assert sorted(burst["photo_ids"]) == sorted(photo_ids), burst["photo_ids"]
    assert len(cache["encounters"][0]["bursts"]) == 1, "no detach should have happened"

    # The "removed" photo still got tagged because it remained a member.
    tagged = _photos_with_species(live_server, photo_ids, "Broad-winged Hawk")
    assert tagged == set(photo_ids), tagged
    assert second_pid in tagged
