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
import time

import pytest
from playwright.sync_api import expect

# Regression guard for a fixed product bug: grmOnToggleChange() used to pin
# BOTH overrides (confirmSpeciesOverride AND applyFlagsOverride) to their
# current DOM state on EVERY checkbox toggle, instead of recording an override
# only for the box the user actually clicked. So the realistic species-only
# flow — uncheck "Apply picks/rejects" first, THEN type a new species — left
# "Confirm species" stuck unchecked even though a real, pending species change
# existed. The fix records an override only for the box that fired (per-box
# override intent), so the two tests below now exercise the intended flow.


def _write_single_burst_cache(
    live_server,
    photo_ids,
    *,
    confirmed_species=None,
    burst_override_species=None,
    burst_override_confirmed=True,
):
    """Write a pipeline cache with one encounter containing one multi-frame
    burst over `photo_ids`. Optionally pre-confirm the burst's species so we
    can test the "same species → no-op, stays unchecked" smart default.

    By default the per-burst `species_override` mirrors `confirmed_species`
    (encounter and burst agree). Pass `burst_override_species` to give the
    burst an override species that DIFFERS from the encounter
    `confirmed_species` — this models a burst confirmed/overridden as X while
    the encounter label is Y, the case the confirm/apply split must not clobber
    when applying flags only. `burst_override_confirmed` toggles whether that
    override is confirmed (defaults True).
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
    if burst_override_species is not None:
        override = {
            "species": burst_override_species,
            "confirmed": burst_override_confirmed,
        }
    elif confirmed_species:
        override = {"species": confirmed_species, "confirmed": True}
    else:
        override = None
    burst = {
        "photo_ids": ids,
        "species_predictions": [],
        "species_override": override,
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


def _dispatch_contextmenu(locator):
    locator.evaluate(
        "el => el.dispatchEvent(new MouseEvent('contextmenu', "
        "{clientX: 100, clientY: 100, bubbles: true, cancelable: true}))"
    )


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


def _tag_all(live_server, photo_ids, species):
    """Tag every photo with `species` as a species keyword directly in the DB,
    so /group/state reports has_species_keyword=True for the whole burst."""
    db = live_server["db"]
    kid = db.add_keyword(species, is_species=True)
    for pid in photo_ids:
        db.tag_photo(pid, kid)
    return kid


def test_pipeline_burst_context_open_browse_falls_back_to_current_window(live_server, page):
    """Open in Browse must still navigate when window.open is ignored."""
    photo_ids = live_server["data"]["photos"][0:3]
    _write_single_burst_cache(live_server, photo_ids)

    _open_burst_modal(page, live_server)
    cards = page.locator("#grmOverlay .grm-card[data-photo-id]")
    assert cards.count() >= 2
    target = cards.nth(1)
    target_pid = target.get_attribute("data-photo-id")
    assert target_pid

    page.evaluate("() => { window.open = () => null; }")
    _dispatch_contextmenu(target)

    selected = page.evaluate("String(grmState.selected)")
    assert selected == target_pid

    menu = page.locator(".vireo-ctx-menu")
    expect(menu).to_be_visible()
    menu.get_by_text("Open in Browse", exact=True).click()

    page.wait_for_function(
        "expectedPid => location.pathname === '/browse'"
        " && new URLSearchParams(location.search).get('photo_id') === expectedPid",
        arg=target_pid,
        timeout=5000,
    )


def test_pipeline_burst_loupe_context_open_browse_uses_selected_photo(live_server, page):
    """Right-clicking the Process Review loupe should target its selected photo."""
    photo_ids = live_server["data"]["photos"][0:3]
    _write_single_burst_cache(live_server, photo_ids)

    _open_burst_modal(page, live_server)
    selected_pid = page.evaluate("String(grmState.selected)")
    assert selected_pid and selected_pid != "null"

    page.evaluate("() => { window.open = () => null; }")
    _dispatch_contextmenu(page.locator("#grmLoupePhoto"))

    menu = page.locator(".vireo-ctx-menu")
    expect(menu).to_be_visible()
    menu.get_by_text("Open in Browse", exact=True).click()

    page.wait_for_function(
        "expectedPid => location.pathname === '/browse'"
        " && new URLSearchParams(location.search).get('photo_id') === expectedPid",
        arg=selected_pid,
        timeout=5000,
    )


def test_smart_default_flags_checked_when_moves_pending(live_server, page):
    """Moving a frame to rejects pre-checks "Apply picks/rejects" via the smart
    default; "Confirm species" stays unchecked when every frame already
    carries the unchanged species."""
    photo_ids = live_server["data"]["photos"][0:3]  # hawk1/2/3, one burst
    _write_single_burst_cache(live_server, photo_ids, confirmed_species="Red-tailed Hawk")
    _tag_all(live_server, photo_ids, "Red-tailed Hawk")

    _open_burst_modal(page, live_server)

    # Reject the currently-selected frame.
    page.keyboard.press("x")
    expect(page.locator("#grmCount")).to_contain_text("1 rejects")

    expect(page.locator("#grmApplyFlagsChk")).to_be_checked()
    expect(page.locator("#grmConfirmSpeciesChk")).not_to_be_checked()


def test_apply_and_close_is_single_flight_while_request_is_pending(live_server, page):
    """Repeated Apply invocations while the first write is in flight must not
    post duplicate group/apply requests."""
    photo_ids = live_server["data"]["photos"][0:3]
    _write_single_burst_cache(live_server, photo_ids)

    apply_requests = []

    def delay_apply(route):
        apply_requests.append(route.request)
        time.sleep(0.2)
        route.continue_()

    page.route("**/api/pipeline/group/apply", delay_apply)
    _open_burst_modal(page, live_server)

    page.keyboard.press("x")
    page.locator("#grmConfirmSpeciesChk").uncheck()  # This case exercises flags only.
    expect(page.locator("#grmApplyFlagsChk")).to_be_checked()

    with page.expect_response("**/api/pipeline/group/apply"):
        page.evaluate(
            """() => {
                grmApply();
                grmApply();
                grmApply();
            }"""
        )

    expect(page.locator("#grmOverlay")).not_to_have_class(re.compile(r"\bopen\b"))
    assert len(apply_requests) == 1


def test_smart_default_species_checked_on_new_species(live_server, page):
    """Typing a species that differs from the burst's confirmed species
    pre-checks "Confirm species"."""
    photo_ids = live_server["data"]["photos"][0:3]
    _write_single_burst_cache(live_server, photo_ids, confirmed_species="Red-tailed Hawk")
    # Tag every frame with the confirmed species so there is no outstanding
    # keyword work — this isolates the "species text unchanged → unchecked"
    # smart default from the missing-keyword gate (covered separately).
    _tag_all(live_server, photo_ids, "Red-tailed Hawk")

    _open_burst_modal(page, live_server)

    # Field opens pre-filled with the confirmed species → no change → unchecked.
    expect(page.locator("#grmSpecies")).to_have_value("Red-tailed Hawk")
    expect(page.locator("#grmConfirmSpeciesChk")).not_to_be_checked()
    expect(page.locator("#grmSpeciesKeywordSummary")).to_contain_text(
        "Species keyword: 3/3 applied"
    )
    expect(page.locator("#grmOverlay .grm-card-needs-tag")).to_have_count(0)
    expect(page.locator("#grmOverlay .grm-card-kw-dot")).to_have_count(0)

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


def test_species_only_apply_leaves_flags_untouched_and_tags_all_frames(live_server, page):
    """Species-only apply (flags unchecked): no photo's flag changes, EVERY
    burst frame gets the species keyword, and the burst is marked confirmed.

    Intended flow: stage a flag move, uncheck "Apply picks/rejects", THEN type
    a new species so "Confirm species" auto-checks."""
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

    page.locator("#grmApplyBtn").click()
    expect(page.locator("#speciesConfirmationPreview .species-preview-apply")).to_be_enabled()
    with page.expect_response(lambda response: '/api/encounters/species' in response.url and not response.request.post_data_json.get('preview')):
        page.locator("#speciesConfirmationPreview .species-preview-apply").click()
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


def test_smart_default_species_checked_when_frame_missing_keyword(live_server, page):
    """Burst already confirmed as the current species, but one frame still
    lacks that species keyword (e.g. legacy data that only tagged picks). The
    species field is unchanged, so speciesChanged is false — but outstanding
    tag work must still flip the "Confirm species" smart default ON, and Apply
    must POST /api/encounters/species so the missing keyword gets written.

    Regression for the classic burst-group modal analogue of the rapid-review
    missing-keyword gate: /api/pipeline/group/apply is flags-only, so without
    this gate the unwritten keyword would be silently dropped.
    """
    db = live_server["db"]
    photo_ids = live_server["data"]["photos"][0:3]
    species = "Red-tailed Hawk"

    # Confirm the burst as the species in the cache, but only tag TWO of the
    # three frames in the DB — the third is missing the keyword.
    _write_single_burst_cache(live_server, photo_ids, confirmed_species=species)
    kid = db.add_keyword(species, is_species=True)
    db.tag_photo(photo_ids[0], kid)
    db.tag_photo(photo_ids[1], kid)
    # photo_ids[2] intentionally left untagged.

    _open_burst_modal(page, live_server)

    # Species field reflects the confirmed species; do NOT change it.
    expect(page.locator("#grmSpecies")).to_have_value(species)
    # Smart default flips ON because a frame still needs the keyword, even
    # though the confirmed species itself is unchanged.
    expect(page.locator("#grmConfirmSpeciesChk")).to_be_checked()
    expect(page.locator("#grmSpeciesKeywordSummary")).to_contain_text(
        "Species keyword: 2/3 applied"
    )
    expect(page.locator("#grmOverlay .grm-card-needs-tag")).to_have_count(1)
    expect(page.locator("#grmOverlay .grm-card-kw-dot")).to_have_count(2)
    expect(page.locator("#grmOverlay .grm-card-kw-badge")).to_have_count(0)

    page.locator("#grmApplyBtn").click()
    expect(page.locator("#speciesConfirmationPreview .species-preview-apply")).to_be_enabled()
    with page.expect_response(lambda response: '/api/encounters/species' in response.url and not response.request.post_data_json.get('preview')):
        page.locator("#speciesConfirmationPreview .species-preview-apply").click()
    expect(page.locator("#grmOverlay")).not_to_have_class(re.compile(r"\bopen\b"))

    # All three frames now carry the species keyword — the missing one got it.
    tagged = _photos_with_species(live_server, photo_ids, species)
    assert tagged == set(photo_ids), tagged


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


def test_flags_only_apply_preserves_differing_burst_override(live_server, page):
    """Regression: a burst CONFIRMED as override species X while the encounter
    label is a different species Y must NOT have X replaced by Y when the user
    applies pick/reject edits only.

    On open the species field must reflect the burst's actual override (X), not
    the encounter label (Y), so "Confirm species" stays UNchecked. Moving a
    frame to rejects and clicking Apply then runs the flags-only path and leaves
    the override species untouched — it does not silently post Y to
    /api/encounters/species (which would untag X, tag Y).
    """
    # Pick species that the conftest fixture does NOT pre-tag onto these frames
    # (it only tags photo[0]="Red-tailed Hawk", photo[3]="American Robin"), so a
    # post-apply keyword check cleanly reflects what THIS apply did.
    override_species = "Cooper's Hawk"      # X — what the burst is confirmed as
    encounter_species = "Northern Harrier"  # Y — the encounter label/prediction
    photo_ids = live_server["data"]["photos"][0:3]
    _write_single_burst_cache(
        live_server,
        photo_ids,
        confirmed_species=encounter_species,
        burst_override_species=override_species,
        burst_override_confirmed=True,
    )
    # Tag every frame with the override species X so there is no outstanding
    # keyword work — this keeps "Confirm species" OFF by the smart default,
    # isolating the override-preservation behavior from the missing-keyword gate.
    _tag_all(live_server, photo_ids, override_species)

    _open_burst_modal(page, live_server)

    # Field reflects the BURST override (X), not the encounter label (Y), and
    # "Confirm species" defaults OFF because field == already-confirmed species.
    expect(page.locator("#grmSpecies")).to_have_value(override_species)
    expect(page.locator("#grmConfirmSpeciesChk")).not_to_be_checked()

    # Reject the selected frame; flags auto-checks. Apply runs flags-only.
    page.keyboard.press("x")
    expect(page.locator("#grmApplyFlagsChk")).to_be_checked()
    expect(page.locator("#grmConfirmSpeciesChk")).not_to_be_checked()

    with page.expect_response("**/api/pipeline/group/apply"):
        page.locator("#grmApplyBtn").click()
    expect(page.locator("#grmOverlay")).not_to_have_class(re.compile(r"\bopen\b"))

    # The reject persisted.
    flags = _photo_flags(live_server, photo_ids)
    assert sorted(flags.values()) == ["none", "none", "rejected"], flags

    # The burst's override species is STILL X (not replaced by Y).
    cache = _read_cache(live_server)
    burst = cache["encounters"][0]["bursts"][0]
    assert burst["species_override"]["species"] == override_species, burst["species_override"]
    assert burst["species_override"]["confirmed"] is True

    # No frame was tagged with the encounter species Y by a stray species call.
    tagged_y = _photos_with_species(live_server, photo_ids, encounter_species)
    assert tagged_y == set(), tagged_y


def test_detached_burst_preserves_differing_confirmed_override(live_server, page):
    """Regression: detaching a frame from a burst that is CONFIRMED as override
    species X (while the encounter label is a different species Y) must carry X
    onto the new single-photo burst — not drop it to null (which silently
    reverts that frame to the encounter species Y on a flags-only apply).

    "Remove from group" one frame, leave "Apply picks/rejects" CHECKED (the
    flags-only detach path runs), Apply → the new single-photo burst in the
    saved cache must retain species_override = {species: X, confirmed: True},
    and the shortened source burst must still carry X too.
    """
    override_species = "Cooper's Hawk"      # X — what the burst is confirmed as
    encounter_species = "Northern Harrier"  # Y — the encounter label/prediction
    photo_ids = live_server["data"]["photos"][0:3]
    _write_single_burst_cache(
        live_server,
        photo_ids,
        confirmed_species=encounter_species,
        burst_override_species=override_species,
        burst_override_confirmed=True,
    )
    # Tag every frame with the override species X so the burst has no
    # outstanding keyword work — confirm stays OFF, isolating the detach-path
    # override-preservation from the missing-keyword gate.
    _tag_all(live_server, photo_ids, override_species)

    _open_burst_modal(page, live_server)

    # Field reflects the burst override (X); confirm defaults OFF (no change).
    expect(page.locator("#grmSpecies")).to_have_value(override_species)
    expect(page.locator("#grmConfirmSpeciesChk")).not_to_be_checked()

    # Remove the second card from the group; keep "Apply picks/rejects" CHECKED
    # so grmApply runs the flags-only detach path that splits it into its own
    # single-photo burst.
    second_pid = int(
        page.locator("#grmOverlay .grm-card[data-photo-id]").nth(1).get_attribute("data-photo-id")
    )
    page.locator(f"#grmOverlay .grm-card[data-photo-id='{second_pid}']").click()
    page.locator("#grmRemoveBtn").click()
    expect(page.locator("#grmApplyFlagsChk")).to_be_checked()
    expect(page.locator("#grmConfirmSpeciesChk")).not_to_be_checked()

    with page.expect_response("**/api/pipeline/group/apply"):
        page.locator("#grmApplyBtn").click()
    expect(page.locator("#grmOverlay")).not_to_have_class(re.compile(r"\bopen\b"))

    cache = _read_cache(live_server)
    bursts = cache["encounters"][0]["bursts"]
    # The detach produced a second burst (the removed frame on its own).
    assert len(bursts) == 2, [b["photo_ids"] for b in bursts]
    detached = next(b for b in bursts if b["photo_ids"] == [second_pid])
    source = next(b for b in bursts if second_pid not in b["photo_ids"])

    # The detached single-photo burst KEEPS the burst-specific confirmed override
    # X — it is not null and not the encounter species Y.
    assert detached["species_override"] is not None, detached
    assert detached["species_override"]["species"] == override_species, detached["species_override"]
    assert detached["species_override"]["confirmed"] is True, detached["species_override"]

    # The shortened source burst still carries X for its remaining frames.
    assert source["species_override"]["species"] == override_species, source["species_override"]
    assert source["species_override"]["confirmed"] is True

    # No frame was tagged with the encounter species Y by a stray species call.
    tagged_y = _photos_with_species(live_server, photo_ids, encounter_species)
    assert tagged_y == set(), tagged_y


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

    page.locator("#grmApplyBtn").click()
    expect(page.locator("#speciesConfirmationPreview .species-preview-apply")).to_be_enabled()
    with page.expect_response(lambda response: '/api/encounters/species' in response.url and not response.request.post_data_json.get('preview')):
        page.locator("#speciesConfirmationPreview .species-preview-apply").click()
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


def test_keyword_summary_counts_discarded_removals(live_server, page):
    """Regression: with "Apply picks/rejects" unchecked, a photo the user marked
    "Remove from group" is still a burst member Apply will tag. The header
    summary must reflect that — showing `N/3` (mixed) instead of `2/2 applied`.

    Bug: the summary and per-card markers were computed from the visible cards
    (removed frames filtered out), so it could show `2/2 applied` with no mixed
    state even though Apply was still going to tag the hidden third frame.
    """
    photo_ids = live_server["data"]["photos"][0:3]
    species = "Red-tailed Hawk"

    # Burst is confirmed as `species`; tag TWO frames with it and leave the
    # third missing the keyword. We'll remove one of the tagged frames so the
    # visible-cards view would look "2/2 applied", while the effective post-
    # apply members (removal discarded) are still 3 with one missing.
    _write_single_burst_cache(live_server, photo_ids, confirmed_species=species)
    db = live_server["db"]
    kid = db.add_keyword(species, is_species=True)
    db.tag_photo(photo_ids[0], kid)
    db.tag_photo(photo_ids[1], kid)
    # photo_ids[2] intentionally untagged.

    _open_burst_modal(page, live_server)

    # Sanity: field pre-fills with the confirmed species; summary shows 2/3.
    expect(page.locator("#grmSpecies")).to_have_value(species)
    expect(page.locator("#grmSpeciesKeywordSummary")).to_contain_text(
        "Species keyword: 2/3 applied"
    )

    # Remove one of the ALREADY-TAGGED frames from the group. Use the second
    # card explicitly: the first card is already selected on modal open, so
    # clicking it would deselect rather than change the anchor (grmSelect's
    # single-click toggle).
    target_pid = int(
        page.locator("#grmOverlay .grm-card[data-photo-id]").nth(1).get_attribute("data-photo-id")
    )
    assert target_pid == photo_ids[1]
    page.locator(f"#grmOverlay .grm-card[data-photo-id='{target_pid}']").click()
    page.locator("#grmRemoveBtn").click()

    # Removal auto-checks flags; UNCHECK to discard the removal so Apply's
    # species call still targets all three frames.
    expect(page.locator("#grmApplyFlagsChk")).to_be_checked()
    page.locator("#grmApplyFlagsChk").uncheck()

    # The header summary must still say `2/3`, not `1/2`, because Apply will
    # still tag the hidden third frame — visible cards no longer include the
    # removed one, but the effective member set does.
    expect(page.locator("#grmSpeciesKeywordSummary")).to_contain_text(
        "Species keyword: 2/3 applied"
    )
    # And the summary must carry the mixed-state warning class, not the "ok"
    # class that would appear for a truly-complete N/N applied state.
    summary = page.locator("#grmSpeciesKeywordSummary")
    expect(summary).to_have_class(re.compile(r"\bwarn\b"))
    expect(summary).not_to_have_class(re.compile(r"\bok\b"))


def test_species_field_clear_is_not_repopulated(live_server, page):
    """Regression: clearing the species field must leave it blank. Previously
    every renderGroupModal() call hit a fallback that copied the encounter's
    confirmed species back into the input, so users couldn't leave the field
    empty (and on unconfirmed predicted bursts the reinstated species could
    apply an unwanted keyword without the user noticing)."""
    photo_ids = live_server["data"]["photos"][0:3]
    species = "Red-tailed Hawk"
    _write_single_burst_cache(live_server, photo_ids, confirmed_species=species)
    _tag_all(live_server, photo_ids, species)

    _open_burst_modal(page, live_server)

    # Field opens pre-filled with the confirmed species.
    field = page.locator("#grmSpecies")
    expect(field).to_have_value(species)

    # Clear it, then wait long enough for the debounced refetch (250ms) plus
    # any follow-up renderGroupModal() calls to fire.
    field.fill("")
    page.wait_for_timeout(400)

    # The field stays blank — the render-time fallback must not resurrect it.
    expect(field).to_have_value("")
    # And the summary reflects "no species", not the stale species stats.
    expect(page.locator("#grmSpeciesKeywordSummary")).to_contain_text(
        "No species keyword"
    )


def test_detached_burst_preserves_explicit_empty_override(live_server, page):
    """A burst whose species were all removed carries the explicit-empty
    override ({confirmed: false, species_list: []}). Detaching a frame from
    it must copy that sentinel onto the new single-photo burst: a null
    override would make the frame inherit the still-confirmed encounter
    species even though its database row carries no species tag."""
    encounter_species = "Northern Harrier"
    photo_ids = live_server["data"]["photos"][0:3]
    _write_single_burst_cache(live_server, photo_ids, confirmed_species=encounter_species)
    empty = {"species": None, "confirmed": False, "species_list": []}
    cache = _read_cache(live_server)
    cache["encounters"][0]["bursts"][0]["species_override"] = dict(empty)
    with open(_cache_path(live_server), "w") as f:
        json.dump(cache, f)

    _open_burst_modal(page, live_server)
    # The sentinel is authoritative: the field must not pre-fill the
    # encounter species, and "Confirm species" must default OFF, or a
    # culling-only apply would re-tag the species that was just removed.
    expect(page.locator("#grmSpecies")).to_have_value("")
    expect(page.locator("#grmConfirmSpeciesChk")).not_to_be_checked()

    second_pid = int(
        page.locator("#grmOverlay .grm-card[data-photo-id]").nth(1).get_attribute("data-photo-id")
    )
    page.locator(f"#grmOverlay .grm-card[data-photo-id='{second_pid}']").click()
    page.locator("#grmRemoveBtn").click()
    expect(page.locator("#grmApplyFlagsChk")).to_be_checked()

    with page.expect_response("**/api/pipeline/group/apply"):
        page.locator("#grmApplyBtn").click()
    expect(page.locator("#grmOverlay")).not_to_have_class(re.compile(r"\bopen\b"))

    cache = _read_cache(live_server)
    bursts = cache["encounters"][0]["bursts"]
    assert len(bursts) == 2, [b["photo_ids"] for b in bursts]
    detached = next(b for b in bursts if b["photo_ids"] == [second_pid])
    source = next(b for b in bursts if second_pid not in b["photo_ids"])
    assert detached["species_override"] == empty, detached["species_override"]
    assert source["species_override"] == empty, source["species_override"]
    assert _photos_with_species(live_server, photo_ids, encounter_species) == set()


def test_detached_burst_preserves_mixed_authoritative_override(live_server, page):
    """A mixed burst's unconfirmed-but-authoritative override
    ({confirmed: false, species_list: [X]}) must be copied onto the
    split-off single-photo burst by the client-side group detach, not
    dropped to null (which would inherit the encounter's stale list)."""
    override_species = "Cooper's Hawk"
    encounter_species = "Northern Harrier"
    photo_ids = live_server["data"]["photos"][0:3]
    _write_single_burst_cache(live_server, photo_ids, confirmed_species=encounter_species)
    mixed = {"species": override_species, "confirmed": False, "species_list": [override_species]}
    cache = _read_cache(live_server)
    cache["encounters"][0]["confirmed_species_list"] = [encounter_species, override_species]
    cache["encounters"][0]["species_confirmed"] = False
    cache["encounters"][0]["bursts"][0]["species_override"] = dict(mixed)
    with open(_cache_path(live_server), "w") as f:
        json.dump(cache, f)
    _tag_all(live_server, photo_ids, override_species)

    _open_burst_modal(page, live_server)
    # The mixed override is the baseline: the untouched field is not a
    # species change, so "Confirm species" defaults OFF.
    expect(page.locator("#grmSpecies")).to_have_value(override_species)
    expect(page.locator("#grmConfirmSpeciesChk")).not_to_be_checked()

    second_pid = int(
        page.locator("#grmOverlay .grm-card[data-photo-id]").nth(1).get_attribute("data-photo-id")
    )
    page.locator(f"#grmOverlay .grm-card[data-photo-id='{second_pid}']").click()
    page.locator("#grmRemoveBtn").click()
    expect(page.locator("#grmApplyFlagsChk")).to_be_checked()
    with page.expect_response("**/api/pipeline/group/apply"):
        page.locator("#grmApplyBtn").click()
    expect(page.locator("#grmOverlay")).not_to_have_class(re.compile(r"\bopen\b"))

    bursts = _read_cache(live_server)["encounters"][0]["bursts"]
    assert len(bursts) == 2, [b["photo_ids"] for b in bursts]
    detached = next(b for b in bursts if b["photo_ids"] == [second_pid])
    assert detached["species_override"] == mixed, detached["species_override"]
    assert _photos_with_species(live_server, photo_ids, encounter_species) == set()


def test_combined_apply_cancel_preserves_flags_membership_and_species(live_server, page):
    """Cancel the preview before either side of the combined apply writes."""
    photo_ids = live_server['data']['photos'][:3]
    _write_single_burst_cache(live_server, photo_ids)
    _open_burst_modal(page, live_server)
    assert page.evaluate('grmState.selected') == photo_ids[0]
    page.keyboard.press('x')
    assert photo_ids[0] in page.evaluate('Array.from(grmState.rejects)')
    page.locator(f'#grmOverlay .grm-card[data-photo-id="{photo_ids[1]}"]').click()
    page.locator('#grmRemoveBtn').click()
    page.locator('#grmSpecies').fill('Northern Goshawk')
    expect(page.locator('#grmApplyFlagsChk')).to_be_checked()
    expect(page.locator('#grmConfirmSpeciesChk')).to_be_checked()
    db = live_server['db']
    before_flags = _photo_flags(live_server, photo_ids)
    before_species = db.photo_species.get_species_for_photos(photo_ids)
    before_cache = _read_cache(live_server)
    before_history = db.conn.execute('SELECT COUNT(*) FROM edit_history').fetchone()[0]
    writes = []
    page.on('request', lambda request: writes.append(request.url)
            if request.method == 'POST' and (
                '/api/pipeline/group/apply' in request.url or
                '/api/encounters/species' in request.url and not request.post_data_json.get('preview')
            ) else None)
    page.locator('#grmApplyBtn').click()
    preview = page.locator('#speciesConfirmationPreview')
    expect(preview.locator('.species-preview-apply')).to_be_enabled()
    preview.locator('.species-preview-cancel').click()
    expect(preview).to_have_count(0)
    expect(page.locator('#grmApplyBtn')).to_be_enabled()
    assert writes == []
    assert _photo_flags(live_server, photo_ids) == before_flags
    assert db.photo_species.get_species_for_photos(photo_ids) == before_species
    assert _read_cache(live_server) == before_cache
    assert db.conn.execute('SELECT COUNT(*) FROM edit_history').fetchone()[0] == before_history
    page.locator('#grmOverlay .grm-close').click()
    expect(page.locator('#grmOverlay')).not_to_have_class(re.compile(r'\bopen\b'))
    assert _photo_flags(live_server, photo_ids) == before_flags
    assert _read_cache(live_server) == before_cache


def test_combined_apply_species_split_relocates_pending_removal(live_server, page):
    """The confirmed leading frame can move before the source burst is culled."""
    ids = live_server['data']['photos'][:4]
    _write_single_burst_cache(live_server, ids)
    _open_burst_modal(page, live_server)
    assert page.evaluate('grmState.selected') == ids[0]
    page.keyboard.press('x')
    assert ids[0] in page.evaluate('Array.from(grmState.rejects)')
    page.locator(f'#grmOverlay .grm-card[data-photo-id="{ids[2]}"]').click()
    page.locator('#grmRemoveBtn').click()
    page.locator('#grmSpecies').fill('Northern Goshawk')
    page.locator('#grmApplyBtn').click()
    preview = page.locator('#speciesConfirmationPreview')
    expect(preview.locator('.species-preview-apply')).to_be_enabled()
    expect(preview.locator('tbody tr')).to_have_count(3)
    for pid in (ids[1], ids[3]):
        preview.locator(f'input[data-photo-id="{pid}"]').uncheck()
        expect(preview.locator('.species-preview-apply')).to_be_enabled()
    preview.locator('.species-preview-split').check()
    expect(preview.locator('.species-preview-apply')).to_be_enabled()
    with page.expect_response(lambda response: '/api/encounters/species' in response.url and not response.request.post_data_json.get('preview')) as response:
        preview.locator('.species-preview-apply').click()
    assert response.value.status == 200
    assert ids[0] in response.value.request.post_data_json['group_changes']['rejects'], response.value.request.post_data_json
    expect(page.locator('#grmOverlay')).not_to_have_class(re.compile(r'\bopen\b'))
    cache = _read_cache(live_server)
    assert [enc['photo_ids'] for enc in cache['encounters']] == [ids[:1], ids[1:]]
    assert {tuple(burst['photo_ids']) for burst in cache['encounters'][1]['bursts']} == {
        (ids[1], ids[3]), (ids[2],),
    }
    assert _photo_flags(live_server, ids)[ids[0]] == 'rejected'
    assert _photos_with_species(live_server, ids, 'Northern Goshawk') == {ids[0]}


@pytest.mark.parametrize('failure', ['network', 'server', 'membership'])
def test_combined_apply_failed_request_never_saves_a_species_only_half(live_server, page, failure):
    ids = live_server['data']['photos'][:3]
    _write_single_burst_cache(live_server, ids)
    _open_burst_modal(page, live_server)
    page.keyboard.press('x')
    page.locator(f'#grmOverlay .grm-card[data-photo-id="{ids[1]}"]').click()
    page.locator('#grmRemoveBtn').click()
    page.locator('#grmSpecies').fill('Northern Goshawk')
    page.locator('#grmApplyBtn').click()
    preview = page.locator('#speciesConfirmationPreview')
    expect(preview.locator('.species-preview-apply')).to_be_enabled()
    db = live_server['db']
    before_flags = _photo_flags(live_server, ids)
    before_species = db.photo_species.get_species_for_photos(ids)
    before_history = db.conn.execute('SELECT COUNT(*) FROM edit_history').fetchone()[0]
    if failure == 'membership':
        cache = _read_cache(live_server)
        source = cache['encounters'][0]['bursts'][0]
        source['photo_ids'].remove(ids[1])
        cache['encounters'][0]['bursts'].append({'photo_ids': [ids[1]]})
        with open(_cache_path(live_server), 'w') as handle:
            json.dump(cache, handle)
    else:
        def fail_write(route):
            if route.request.post_data_json.get('preview'):
                route.continue_()
            elif failure == 'network':
                route.abort('failed')
            else:
                route.fulfill(status=503, json={'error': 'Injected Group Review failure'})
        page.route('**/api/encounters/species', fail_write)
    before_cache = _read_cache(live_server)
    standalone_flags = []
    page.on('request', lambda request: standalone_flags.append(request.url)
            if '/api/pipeline/group/apply' in request.url else None)
    preview.locator('.species-preview-apply').click()
    expect(page.locator('#grmApplyBtn')).to_be_enabled()
    expect(page.locator('#grmOverlay')).to_have_class(re.compile(r'\bopen\b'))
    assert standalone_flags == []
    assert _photo_flags(live_server, ids) == before_flags
    assert db.photo_species.get_species_for_photos(ids) == before_species
    assert _read_cache(live_server) == before_cache
    assert db.conn.execute('SELECT COUNT(*) FROM edit_history').fetchone()[0] == before_history
